use super::*;

#[derive(Clone, Copy)]
struct Boundary {
    low: u64,
    high: u64,
}

fn anchor(case: &Workload) -> bool {
    matches!(case, Workload::W1 {bytes:64, ..})
        || matches!(case, Workload::W2 {connections:1, split} if split.to_peer == 1 && split.to_application == 1)
}

fn rate(t: &Trial, e: &Evidence) -> Result<u64> {
    let split = t.workload.dimensions().2;
    streams(t, e, 1)?.iter().filter(|s| s.rate > 0).map(|s| {
        Ok(n(s.rx,"validated_in_window")? / t.measure / if s.to_peer {split.to_peer} else {split.to_application})
    }).collect::<Result<Vec<_>>>()?.into_iter().min().ok_or_else(|| "no active capacity stream".into())
}

fn sufficient(t: &Trial, e: &Evidence, p: &Profile) -> Result<()> {
    for s in streams(t,e,1)? {
        if s.rate > 0 {
            require(n(s.rx,"validated_in_window")? >= n(&p.raw,"minimum_samples")?,
                "inconclusive: fewer than 10000 measured observations in an active stream")?;
        }
    }
    Ok(())
}

fn rejection_boundary(t: &Trial, e: &Evidence, p: &Profile) -> bool {
    if e.clients.len()!=1 || correctness(t,e,p,true).is_err() {return false;}
    let failure = &e.clients[0]["failure"];
    let peer = &e.peer["failure"];
    let transport_failure = if t.path==PathKind::Reference {failure["kind"]=="transport"} else {failure["kind"]=="adapter" && failure["code"]==22};
    if transport_failure && peer["kind"]=="sequence" && peer["direction"]==0 {
        let (Some(connection),Some(epoch),Some(expected),Some(sequence)) = (peer["connection"].as_u64(),peer["epoch"].as_u64(),peer["expected"].as_u64(),peer["sequence"].as_u64()) else {return false;};
        return sequence>expected && row(&e.clients[0],connection as usize,epoch).is_ok_and(|stream| {
            let rejected = n(&stream["tx"],"rejected").ok();
            let accounted = if t.path==PathKind::Reference {rejected==Some(sequence-expected)} else {
                rejected.is_some_and(|v|v>=sequence-expected)
                    && stream["tx"]["rejections"].as_array().is_some_and(|ranges|ranges.iter().any(|range| {
                        range[0].as_u64().is_some_and(|start|start<=expected)
                            && range[1].as_u64().is_some_and(|end|end>=sequence)
                    }))
            };
            n(stream,"queue_highwater_descriptors").ok()==Some(4096) && accounted
        });
    }
    if failure["kind"]!="sequence" || failure["direction"]!=1 || e.peer["failure"]["kind"]!="transport" {return false;}
    let (Some(connection),Some(epoch),Some(expected),Some(sequence)) = (failure["connection"].as_u64(),failure["epoch"].as_u64(),failure["expected"].as_u64(),failure["sequence"].as_u64()) else {return false;};
    if sequence<=expected {return false;}
    row(&e.peer,connection as usize,epoch).is_ok_and(|stream| {
        n(stream,"queue_highwater_descriptors").ok()==Some(4096)
            && n(&stream["tx"],"rejected").is_ok_and(|v|v>=sequence-expected)
    })
}

fn boundary_outcome(t: &Trial, e: &Evidence, p: &Profile, gate: &std::result::Result<(),CapacityFailure>) -> Result<bool> {
    match gate {
        Ok(()) => Ok(true),
        Err(CapacityFailure::Latency | CapacityFailure::Backlog) => Ok(false),
        Err(CapacityFailure::Correctness(_)) if rejection_boundary(t,e,p) => Ok(false),
        Err(error) => Err(format!("inconclusive: invalid capacity boundary evidence: {error}").into()),
    }
}

impl Controller {
    fn bounded_record(&mut self, t: &Trial, e: &Evidence, label: &str, gate: Result<()>) -> Result<()> {
        self.record(t,e,label)?;
        let failure = gate.as_ref().err().map(ToString::to_string);
        self.records.last_mut().ok_or("trial record")?["gate_error"] = json!(failure);
        self.records.last_mut().ok_or("trial record")?["warm_s"] = json!(t.warm);
        self.records.last_mut().ok_or("trial record")?["measure_s"] = json!(t.measure);
        self.records.last_mut().ok_or("trial record")?["streams"] = json!(streams(t,e,1)?.iter().filter(|s|s.rate > 0).map(|s| {
            json!({"connection":s.rx["id"],"to_peer":s.to_peer,"offered_rate":s.rate,"validated_in_window":s.rx["validated_in_window"],"p99_us":p99(s.rx,"latency_histogram_us",n(s.rx,"validated").unwrap_or(0)).ok()})
        }).collect::<Vec<_>>());
        save(&self.out.join("trials.json"),&json!(self.records))?;
        gate
    }

    fn bounded_search(&mut self, path: PathKind, case: &Workload, hint: Option<Boundary>, spent: &mut Duration) -> Result<Boundary> {
        let started = Instant::now();
        let result = (|| {
            let initial = match path {PathKind::Reference=>200000,PathKind::Native=>100000,PathKind::Bend=>15000};
            let mut low = 0;
            let mut high = hint.map_or(initial,|b|b.low);
            let mut bracketed = false;
            for step in 0..32 {
                require(*spent + started.elapsed() < Duration::from_secs(n(&self.profile.raw,"calibration_seconds")?),
                    "inconclusive: bounded calibration budget exhausted")?;
                let offered = if bracketed {low+(high-low)/2} else {high};
                let (t,e) = self.run_case(path,case.clone(),offered,1,3)?;
                let gate = capacity_gate(&t,&e,&self.profile,2000);
                let reason = gate.as_ref().err().map(ToString::to_string);
                self.record(&t,&e,"fresh boundary probe, not qualification")?;
                self.records.last_mut().ok_or("search record")?["gate_error"] = json!(reason);
                save(&self.out.join("trials.json"),&json!(self.records))?;
                let passed = boundary_outcome(&t,&e,&self.profile,&gate)?;
                if passed {
                    low = offered;
                    if !bracketed {
                        let next = if step == 0 {hint.map_or(offered*5/4,|b|b.high)} else {offered*2};
                        require(next > offered && next <= n(&self.profile.raw,"search_ceiling_rate")?,
                            "inconclusive: candidate ceiling without failing boundary")?;
                        high = next;
                    }
                } else {
                    high = offered;
                    bracketed = true;
                }
                if bracketed && low > 0 && high-low <= (low/100).max(1) {
                    let boundary = Boundary {low,high};
                    save(&self.out.join(format!("boundary-{:04}.json",self.next_trial)),
                        &json!({"path":format!("{path:?}"),"workload":format!("{case:?}"),"passing_rate":low,"failing_rate":high,"relative_resolution":(high-low) as f64/low as f64,"qualified":false}))?;
                    return Ok(boundary);
                }
            }
            Err("inconclusive: fresh boundary search step limit".into())
        })();
        *spent += started.elapsed();
        result
    }

    fn bounded_anchor(&mut self, case: &Workload, spent: &mut Duration) -> Result<[u64;3]> {
        let paths = [PathKind::Reference,PathKind::Native,PathKind::Bend];
        let mut boundary = [Boundary {low:0,high:0};3];
        for (i,path) in paths.into_iter().enumerate() {
            boundary[i] = self.bounded_search(path,case,None,spent)?;
            boundary[i] = self.bounded_sustained_boundary(path,case,boundary[i])?;
        }
        let mut values: [Vec<u64>;3] = Default::default();
        let mut reference_boundaries = vec![];
        let mut complete = false;
        for refinement in 0..=n(&self.profile.raw,"qualification_refinements")? {
            for v in &mut values {v.clear();}
            reference_boundaries.clear();
            let mut failed = None;
            for pair in 0..5 {
                if pair > 0 {
                    let b = boundary[0];
                    let low_pass = self.bounded_boundary_trial(PathKind::Reference,case,b.low)?;
                    let high_pass = self.bounded_boundary_trial(PathKind::Reference,case,b.high)?;
                    if !low_pass || high_pass {failed=Some(0);break;}
                }
                reference_boundaries.push([boundary[0].low,boundary[0].high]);
                for i in if pair%2 == 0 {[0,1,2]} else {[2,1,0]} {
                    let (t,e) = self.run_case(paths[i],case.clone(),boundary[i].low,5,20)?;
                    let gate = capacity_gate(&t,&e,&self.profile,2000);
                    let outcome = boundary_outcome(&t,&e,&self.profile,&gate);
                    self.bounded_record(&t,&e,"anchor capacity",Ok(()))?;
                    let record = self.records.last_mut().ok_or("paired record")?;
                    record["gate_error"] = json!(gate.as_ref().err().map(ToString::to_string));
                    record["refinement"] = json!(refinement);
                    save(&self.out.join("trials.json"),&json!(self.records))?;
                    if !outcome? {failed=Some(i);break;}
                    sufficient(&t,&e,&self.profile)?;
                    values[i].push(rate(&t,&e)?);
                }
                if failed.is_some() {break;}
            }
            if let Some(i) = failed {
                require(refinement<n(&self.profile.raw,"qualification_refinements")?,"inconclusive: sustained comparison drift after frozen refinement limit")?;
                boundary[i]=self.bounded_sustained_boundary(paths[i],case,boundary[i])?;
            } else {complete=true;break;}
        }
        require(complete && values.iter().all(|v|v.len()==5),"five complete sustained comparison sets")?;
        let medians = [median(&values[0]),median(&values[1]),median(&values[2])];
        let lows = reference_boundaries.iter().map(|b|b[0]).collect::<Vec<_>>();
        let reference_spread = (reference_boundaries.iter().map(|b|b[1]).max().unwrap()-*lows.iter().min().unwrap()) as f64 / median(&lows) as f64;
        let mut ratios = vec![];
        for (a,b) in [(0,1),(1,2)] {
            ratios.push(json!({"comparison":format!("{:?}/{:?}",paths[b],paths[a]),"median":medians[b] as f64/medians[a] as f64,
                "pairs":values[a].iter().zip(&values[b]).map(|(a,b)|*b as f64/ *a as f64).collect::<Vec<_>>()}));
        }
        save(&self.out.join(format!("anchor-{:04}.json",self.next_trial)),&json!({"workload":format!("{case:?}"),"capacities":medians,"values":values,"reference_boundaries":reference_boundaries,"reference_spread_upper_bound":reference_spread,"ratios":ratios,"phase_pass":false}))?;
        require(reference_spread <= 0.05, &format!("inconclusive: reference boundary spread {reference_spread:.4} exceeds 0.05"))?;
        for (a,b) in [(0,1),(1,2)] {
            let ratio = medians[b] as f64 / medians[a] as f64;
            require(ratio >= 0.80,&format!("{:?}/{:?} median capacity ratio {ratio:.4} below 0.80",paths[b],paths[a]))?;
            for (pair,(a_value,b_value)) in values[a].iter().zip(&values[b]).enumerate() {
                let ratio = *b_value as f64 / *a_value as f64;
                require(ratio >= 0.75,&format!("{:?}/{:?} pair {pair} capacity ratio {ratio:.4} below 0.75",paths[b],paths[a]))?;
            }
            let half = *values[a].iter().chain(&values[b]).min().unwrap()/2;
            for pair in 0..5 {
                for i in if pair%2 == 0 {[a,b]} else {[b,a]} {
                    let (t,e) = self.run_case(paths[i],case.clone(),half,5,10)?;
                    let limit = if paths[i] == PathKind::Native {1000} else {2000};
                    let gate = capacity_gate(&t,&e,&self.profile,limit).map_err(|e|e.into()).and_then(|_|sufficient(&t,&e,&self.profile));
                    self.bounded_record(&t,&e,"anchor half-load latency",gate)?;
                }
            }
        }
        let headroom = (*medians.iter().max().unwrap()*5+3)/4;
        let (t,e) = self.trial(Trial {path:PathKind::Reference,workload:case.clone(),rates:case.dimensions().2.scaled(headroom),warm:5,measure:20,token:now(),start:0,tcp:true,fault:false})?;
        let gate = capacity_gate(&t,&e,&self.profile,2000).map_err(|e|e.into()).and_then(|_|sufficient(&t,&e,&self.profile));
        self.bounded_record(&t,&e,"anchor 1.25x TCP headroom",gate)?;
        for s in streams(&t,&e,1)? {
            if s.rate>0 {require(p99(s.tx,"lateness_histogram_us",n(s.tx,"attempted")?)? <= 2000,"inconclusive: headroom generator lateness")?;}
        }
        require(!self.records.iter().any(|r|r["verdict"] == "anchor capacity" && r["peer_cpu_fraction"].as_f64().is_some_and(|v|v>0.8)),"inconclusive: WebSocket peer CPU ceiling")?;
        Ok(medians)
    }

    fn bounded_boundary_trial(&mut self, path: PathKind, case: &Workload, offered: u64) -> Result<bool> {
        let (t,e) = self.run_case(path,case.clone(),offered,5,20)?;
        let gate = capacity_gate(&t,&e,&self.profile,2000);
        let reason = gate.as_ref().err().map(ToString::to_string);
        self.record(&t,&e,"sustained capacity boundary, not a paired sample")?;
        let record = self.records.last_mut().ok_or("sustained boundary record")?;
        record["gate_error"] = json!(reason);
        record["warm_s"] = json!(t.warm);
        record["measure_s"] = json!(t.measure);
        save(&self.out.join("trials.json"),&json!(self.records))?;
        let passed = boundary_outcome(&t,&e,&self.profile,&gate)?;
        if passed {sufficient(&t,&e,&self.profile)?;}
        Ok(passed)
    }

    fn bounded_sustained_boundary(&mut self, path: PathKind, case: &Workload, hint: Boundary) -> Result<Boundary> {
        let mut low = 0;
        let mut high = 0;
        let mut offered = hint.low;
        for _ in 0..n(&self.profile.raw,"search_max_steps")? {
            if self.bounded_boundary_trial(path,case,offered)? {
                low = offered;
                if high == 0 {offered = if hint.high > offered {hint.high} else {offered + offered/4 + 1};}
            } else {high = offered;}
            if low > 0 && high > low {
                if high-low <= (low/100).max(1) {
                    save(&self.out.join(format!("sustained-boundary-{:04}.json",self.next_trial)),
                        &json!({"path":format!("{path:?}"),"workload":format!("{case:?}"),"passing_rate":low,"failing_rate":high,"warm_s":5,"measure_s":20,"qualified_duration":true,"phase_pass":false}))?;
                    return Ok(Boundary {low,high});
                }
                offered = low+(high-low)/2;
            } else if high > 0 {offered = high*9/10;}
            require(offered > 0 && offered <= n(&self.profile.raw,"search_ceiling_rate")?,"inconclusive: sustained capacity ceiling or zero rate")?;
        }
        Err("inconclusive: sustained capacity boundary search step limit".into())
    }

    fn bounded_cancellation_control(&mut self) -> Result<()> {
        let out = self.out.join("cancellation-control");
        fs::create_dir(&out)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        let trial = Trial {path:PathKind::Reference,workload:Workload::W5,rates:Rates {to_peer:1000,to_application:1000},warm:0,measure:30,token:now(),start:0,tcp:true,fault:false};
        let mut command = Command::new(&self.peer);
        self.reference_args(&mut command,&trial,port,&out.join("peer.json"));
        let cancellation = Arc::new(AtomicBool::new(false));
        self.stop = Some(cancellation.clone());
        let peer = self.spawn(command,&out,"peer",Some(self.profile.cpus[1]));
        self.stop = None;
        let mut peer = peer?;
        peer.ready("LISTENING")?;
        let pid = peer.child.id() as i32;
        let started = Instant::now();
        cancellation.store(true,Ordering::Release);
        let result = peer.wait(Instant::now()+Duration::from_secs(2));
        require(result.as_ref().is_err_and(|e|e.to_string().contains("cancelled after lifecycle failure")),"failed lifecycle did not cancel waiting peer")?;
        drop(peer);
        require(unsafe {libc::kill(pid,0)} == -1 && std::io::Error::last_os_error().raw_os_error()==Some(libc::ESRCH),"cancelled peer survived process-group cleanup")?;
        save(&out.join("summary.json"),&json!({"passed":true,"injected":"lifecycle cancellation signal while real reference peer waits","cancel_and_cleanup_s":started.elapsed().as_secs_f64(),"peer_pid":pid,"peer_reaped":true,"phase_pass":false}))
    }

    pub(super) fn bounded_qualification(&mut self) -> Result<Verdict> {
        self.qualification_running = true;
        let started = Instant::now();
        require(n(&self.profile.raw,"half_warm_s")?==5 && n(&self.profile.raw,"half_measure_s")?==10
            && n(&self.profile.raw,"screen_warm_s")?==3 && n(&self.profile.raw,"screen_measure_s")?==5
            && n(&self.profile.raw,"w5_seconds")?==180 && n(&self.profile.raw,"w4_repetitions")?==1
            && self.profile.raw["median_ratio"]==0.80 && self.profile.raw["each_pair_ratio"]==0.75
            && self.profile.raw["reference_spread"]==0.05 && self.profile.raw["headroom_multiplier"]==1.25,
            "frozen bounded qualification policy")?;
        let mut tools = vec![];
        for (name,environment) in [("perf","WS_PHASE5_PERF"),("strace","WS_PHASE5_STRACE")] {
            let program = env::var_os(environment).unwrap_or_else(||name.into());
            let mut command = Command::new(&program);
            command.arg("--version");
            tools.push(json!({"name":name,"program":program,"version":self.build_command(command,&format!("qualification-tool-{name}"))?}));
        }
        save(&self.out.join("tools.json"),&json!(tools))?;
        let cases = self.profile.core()?;
        let mut anchors = cases.iter().filter(|c|anchor(c)).cloned().collect::<Vec<_>>();
        anchors.sort_by_key(|c|!matches!(c,Workload::W2 {..}));
        require(anchors.len()==3 && cases.len()==13,"approved anchor/screen coverage")?;
        save(&self.out.join("coverage.json"),&json!({"anchors":anchors.iter().map(|c|format!("{c:?}")).collect::<Vec<_>>(),
            "screens":cases.iter().filter(|c|!anchor(c)).map(|c|format!("{c:?}")).collect::<Vec<_>>(),"reused":"Phases 1-4 unchanged protocol, ownership, million-message and repeated pause/lifecycle qualification", "numerical_complete":false}))?;
        let mut spent = Duration::ZERO;
        let mut qualified = vec![];
        for case in &anchors {
            println!("Phase 5 anchor: {case:?}");
            save(&self.out.join("progress.json"),&json!({"case":format!("{case:?}"),"elapsed_s":started.elapsed().as_secs_f64(),"calibration_s":spent.as_secs_f64(),"completed_anchors":qualified.len()}))?;
            match self.bounded_anchor(case,&mut spent) {
                Ok(capacities) => qualified.push((case.clone(),capacities)),
                Err(error) => {
                    let reason = error.to_string();
                    let attempted = [PathKind::Reference,PathKind::Native,PathKind::Bend].map(|path| {
                        self.records.iter().rev().find(|r|r["path"] == format!("{path:?}") && r["workload"] == format!("{case:?}") && r["statuses"].as_array().is_some_and(|s|s.iter().all(|v|v==0)))
                            .and_then(|r|r["rates"][0].as_u64().filter(|v|*v>0).or(r["rates"][1].as_u64())).unwrap_or(1000)
                    });
                    let collection = if self.campaign_deadline.is_none_or(|d|Instant::now()+Duration::from_secs(120)<d) {
                        self.limiter_evidence(case,attempted).map(|missing|json!({"missing":missing}))
                    } else {Ok(json!({"skipped":"insufficient campaign time for failure diagnostics"}))};
                    save(&self.out.join("failure-diagnostics.json"),&match collection {Ok(report)=>report,Err(e)=>json!({"collection_error":e.to_string()})})?;
                    save(&self.out.join("campaign.json"),&json!({"numeric_error":reason,"numerical_complete":false,"elapsed_s":started.elapsed().as_secs_f64(),"calibration_s":spent.as_secs_f64(),"completed_anchors":qualified.len()}))?;
                    return if reason.starts_with("inconclusive:") {Ok(Verdict::Inconclusive(reason))} else {Ok(Verdict::Failed(reason))};
                }
            }
        }
        let duplex = qualified.iter().find(|(c,_)|matches!(c,Workload::W2 {..})).ok_or("duplex anchor")?.1;
        for case in cases.iter().filter(|c|!anchor(c)) {
            let (_,connections,split) = case.dimensions();
            for (i,path) in [PathKind::Reference,PathKind::Native,PathKind::Bend].into_iter().enumerate() {
                let offered = duplex[i]/2/connections as u64/split.to_peer.max(split.to_application).max(1);
                require(offered>0,"inconclusive: screening rate rounds to zero")?;
                let (t,e) = self.run_case(path,case.clone(),offered,3,5)?;
                let gate = capacity_gate(&t,&e,&self.profile,2000).map_err(|e|e.into());
                self.bounded_record(&t,&e,"screen only, no capacity claim",gate)?;
            }
        }
        for (i,path) in [PathKind::Native,PathKind::Bend].into_iter().enumerate() {
            for pause in [10,100,1000] {
                for receiver in [0,1] {self.scripted(path,pause,receiver,duplex[i+1]*5,false)?;}
            }
            self.soak(path,duplex[i+1]/2)?;
        }
        self.bounded_cancellation_control()?;
        let mut missing = vec![];
        for (case,capacities) in qualified {missing.extend(self.limiter_evidence(&case,capacities)?);}
        let hashes = measurement_hashes(&self.out,&self.root)?;
        save(&self.out.join("campaign.json"),&json!({"numerical_complete":true,"elapsed_s":started.elapsed().as_secs_f64(),"calibration_s":spent.as_secs_f64(),"missing_assessment":missing,"measurement_hashes":hashes}))?;
        if !missing.is_empty() {return Ok(Verdict::Inconclusive(missing.join("; ")));}
        Ok(Verdict::Passed)
    }

    pub(super) fn bounded_failure_diagnostics(&mut self) -> Result<()> {
        if self.out.join("failure-diagnostics.json").exists() {return Ok(());}
        if self.campaign_deadline.is_some_and(|d|Instant::now()+Duration::from_secs(120)>=d) {
            return save(&self.out.join("failure-diagnostics.json"),&json!({"skipped":"insufficient campaign time"}));
        }
        let cases = self.profile.core()?;
        let case = self.records.last().and_then(|r|cases.iter().find(|c|r["workload"]==format!("{c:?}"))).cloned()
            .unwrap_or(Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}});
        let attempted = [PathKind::Reference,PathKind::Native,PathKind::Bend].map(|path|self.records.iter().rev()
            .find(|r|r["path"]==format!("{path:?}") && r["workload"]==format!("{case:?}"))
            .and_then(|r|r["rates"][0].as_u64().filter(|v|*v>0).or(r["rates"][1].as_u64())).unwrap_or(1000));
        let report = match self.limiter_evidence(&case,attempted) {Ok(missing)=>json!({"missing":missing}),Err(e)=>json!({"collection_error":e.to_string()})};
        save(&self.out.join("failure-diagnostics.json"),&report)
    }
}

pub(super) fn diagnostic_hashes(diagnostics: &[Value]) -> Result<String> {
    let mut files = vec![];
    for diagnostic in diagnostics {
        let directory = Path::new(diagnostic["directory"].as_str().ok_or("diagnostic directory")?);
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_file() {files.push(path);}
        }
    }
    files.sort();
    if files.is_empty() {return Ok(String::new());}
    checksums(&files)
}

fn measurement_hashes(out: &Path, root: &Path) -> Result<String> {
    fn collect(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {collect(&path,files)?;}
            else if path.extension().is_some_and(|s|s=="json") && !matches!(path.file_name().and_then(|s|s.to_str()),Some("summary.json"|"campaign.json"|"assessment-result.json"|"limiter-assessments.json")) {files.push(path);}
        }
        Ok(())
    }
    let mut files = vec![root.join("tests/ws/phase5.rs"),root.join("tests/ws/phase5_bounded.rs"),out.join("cancellation-control/summary.json")];
    collect(out,&mut files)?;
    files.sort();
    checksums(&files)
}

fn bound_assessment(report: &Value, assessment: &Value, root: &Path) -> Result<()> {
    require(assessment["profile"]==report["profile"] && assessment["capacities"]==report["capacities"] && assessment["workload"]==report["workload"],"assessment profile/capacity/workload mismatch")?;
    let diagnostics = report["diagnostics"].as_array().ok_or("diagnostic manifest")?;
    require(report["artifact_hashes"]==diagnostic_hashes(diagnostics)?,"diagnostic artifact changed after collection")?;
    let actors = assessment["actors"].as_array().ok_or("assessment actors")?;
    for path in ["Reference","Native","Bend"] {
        for actor in ["client-0","peer"] {
            let entries = actors.iter().filter(|a|a["path"]==path && a["actor"]==actor).collect::<Vec<_>>();
            require(entries.len()==1,"assessment actor missing or duplicated")?;
            let entry = entries[0];
            for field in ["limiter","why_not_double","source","ruled_out_generator_and_peer"] {
                require(entry[field].as_str().is_some_and(|s|!s.trim().is_empty()),"empty limiter assessment")?;
            }
            let evidence = entry["evidence"].as_array().ok_or("actor assessment evidence")?;
            for (kind,tool,suffix) in [("profile","perf","-cpu-profile.txt"),("counters","perf-stat",".perf-stat.txt"),("syscalls","strace",".strace.txt"),("actor-report","perf",".json")] {
                let references = evidence.iter().filter(|r|r["kind"]==kind).collect::<Vec<_>>();
                require(references.len()==1,"assessment evidence kind missing or duplicated")?;
                let reference = references[0];
                let file = root.join(reference["file"].as_str().ok_or("assessment file")?).canonicalize()?;
                let observation = reference["observation"].as_str().ok_or("assessment observation")?;
                require(!observation.trim().is_empty() && fs::read_to_string(&file)?.contains(observation),"assessment observation absent from artifact")?;
                require(diagnostics.iter().any(|d|d["path"]==path && d["tool"]==tool && d["directory"].as_str().is_some_and(|directory|Path::new(directory).join(format!("{actor}{suffix}")).canonicalize().is_ok_and(|expected|expected==file))),"assessment references another actor or collection")?;
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "focused real-peer cancellation and process-group cleanup control"]
fn phase5_cancellation_control() -> Result<()> {
    Controller::prepare(true)?.bounded_cancellation_control()
}

#[test]
#[ignore = "focused real-actor limiter collection; no capacity or phase pass"]
fn phase5_limiter_probe() -> Result<()> {
    let mut c = Controller::prepare(false)?;
    let case = Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}};
    let rates = [100000,100000,env::var("WS_PHASE5_RATE").map_or(Ok(30000),|v|v.parse::<u64>())?];
    let missing = c.limiter_evidence(&case,rates)?;
    save(&c.out.join("summary.json"),&json!({"scope":"diagnostic only","phase_pass":false,"offered_rates":rates,"missing":missing,"trials":c.records}))
}

#[test]
#[ignore = "one selected path's sustained boundary; no paired ratio or phase pass"]
fn phase5_sustained_probe() -> Result<()> {
    let path = match env::var("WS_PHASE5_PATH")?.as_str() {
        "Reference" => PathKind::Reference,
        "Native" => PathKind::Native,
        "Bend" => PathKind::Bend,
        _ => return Err("WS_PHASE5_PATH must be Reference, Native, or Bend".into()),
    };
    let low = env::var("WS_PHASE5_RATE")?.parse::<u64>()?;
    require(low>0,"positive sustained probe rate")?;
    let mut c = Controller::prepare(false)?;
    let case = Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}};
    let b = c.bounded_sustained_boundary(path,&case,Boundary {low,high:low+low/4+1})?;
    save(&c.out.join("summary.json"),&json!({"scope":"single sustained boundary","phase_pass":false,"path":format!("{path:?}"),"passing_rate":b.low,"failing_rate":b.high,"trials":c.records}))
}

#[test]
#[ignore = "real retained rejection boundary and evidence-damage controls without new traffic"]
fn phase5_rejection_evidence() -> Result<()> {
    let dir = PathBuf::from(env::var_os("WS_PHASE5_EVIDENCE").ok_or("WS_PHASE5_EVIDENCE required")?).canonicalize()?;
    let meta: Value = serde_json::from_slice(&fs::read(dir.join("trial.json"))?)?;
    let path = match meta["path"].as_str() {Some("Reference")=>PathKind::Reference,Some("Native")=>PathKind::Native,Some("Bend")=>PathKind::Bend,_=>return Err("rejection evidence path".into())};
    let t = Trial {path,workload:Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}},rates:rates(&meta["rates"])? ,warm:n(&meta,"warm")?,measure:n(&meta,"measure")?,token:n(&meta,"token")?,start:n(&meta,"start")?,tcp:false,fault:false};
    let trials: Value = serde_json::from_slice(&fs::read(dir.parent().ok_or("campaign directory")?.join("trials.json"))?)?;
    let record = trials.as_array().ok_or("trial records")?.iter().find(|r|r["evidence"]==dir.to_string_lossy().as_ref()).ok_or("recorded actor statuses")?;
    let mut e = Evidence {clients:vec![serde_json::from_slice(&fs::read(dir.join("client-0.json"))?)?],peer:serde_json::from_slice(&fs::read(dir.join("peer.json"))?)?,statuses:serde_json::from_value(record["statuses"].clone())?,directory:dir};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let profile = Profile {raw:serde_json::from_slice(&fs::read(root.join("bench/ws/profiles/local-perf-v2.json"))?)?,cpus:[0,1]};
    require(rejection_boundary(&t,&e,&profile),"accounted real rejection not recognized")?;
    let original = e.clients[0].clone();
    let original_peer = e.peer.clone();
    for damage in ["rejections","queue","failure","token","count"] {
        e.clients[0]=original.clone();e.peer=original_peer.clone();
        match damage {
            "rejections" => for actor in [&mut e.clients[0],&mut e.peer] {for stream in actor["streams"].as_array_mut().ok_or("streams")? {stream["tx"]["rejected"]=json!(0);stream["tx"]["rejections"]=json!([]);}},
            "queue" => for actor in [&mut e.clients[0],&mut e.peer] {for stream in actor["streams"].as_array_mut().ok_or("streams")? {stream["queue_highwater_descriptors"]=json!(1);}},
            "failure" => e.clients[0]["failure"]["kind"]=json!("unaccounted"),
            "count" => e.clients[0]["streams"][1]["rx"]["validated"]=json!(u64::MAX),
            _ => e.clients[0]["token"]=json!(0),
        }
        require(!rejection_boundary(&t,&e,&profile),&format!("damaged {damage} established boundary"))?;
    }
    Ok(())
}

#[test]
#[ignore = "five paired dense-versus-baseline Bend trials on the same endpoint CPU budget"]
fn phase5_dense_comparison() -> Result<()> {
    let baseline = PathBuf::from(env::var_os("WS_PHASE5_BASELINE_DIR").ok_or("WS_PHASE5_BASELINE_DIR required")?).canonicalize()?;
    let manifest:Value = serde_json::from_slice(&fs::read(baseline.join("complete.json"))?)?;
    let artifacts = [baseline.join("uws-reference"),baseline.join("phase5-native"),baseline.join("bend-phase5")];
    require(manifest["artifacts"]==checksums(&artifacts)?,"baseline artifact identity changed")?;
    let marker = manifest["identity"]["marker"].as_str().ok_or("baseline build marker")?;
    require(fs::read(&artifacts[2])?.windows(marker.len()).any(|b|b==marker.as_bytes()),"baseline actor does not match recorded native build")?;
    let mut c = Controller::prepare(false)?;
    let candidate = c.bend.clone();
    let mut pairs = vec![];
    for pair in 0..5 {
        let token = now();
        let mut trials = vec![];
        for version in if pair%2==0 {["baseline","dense"]} else {["dense","baseline"]} {
            c.bend = if version=="baseline" {artifacts[2].clone()} else {candidate.clone()};
            let (t,e) = c.trial(Trial {path:PathKind::Bend,workload:Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}},rates:Rates {to_peer:12000,to_application:12000},warm:5,measure:20,token,start:0,tcp:false,fault:false})?;
            let gate = capacity_gate(&t,&e,&c.profile,2000).map_err(|e|e.into()).and_then(|_|sufficient(&t,&e,&c.profile));
            c.bounded_record(&t,&e,&format!("{version} paired fixed-load Bend comparison"),gate)?;
            trials.push(json!({"version":version,"evidence":e.directory,"stream_p99_us":streams(&t,&e,1)?.iter().map(|s|p99(s.rx,"latency_histogram_us",n(s.rx,"validated").unwrap_or(0))).collect::<Result<Vec<_>>>()?,"client_cpu_ns":e.clients[0]["cpu_ticks"],"resources":e.clients[0]["resources"]}));
        }
        pairs.push(json!({"pair":pair,"token":token,"trials":trials}));
        save(&c.out.join("comparison.json"),&json!({"scope":"paired fixed-load message-path comparison, not capacity qualification","offered_per_direction":12000,"warm_s":5,"measure_s":20,"baseline_manifest":baseline.join("complete.json"),"baseline_actor":artifacts[2],"dense_actor":candidate,"phase_pass":false,"pairs":pairs}))?;
    }
    save(&c.out.join("summary.json"),&json!({"scope":"paired dense Bend message-path comparison","comparison_pass":true,"phase_pass":false,"pairs":pairs,"baseline_manifest":baseline.join("complete.json"),"fixture_identity":"build/identity.json"}))
}

#[test]
#[ignore = "verify saved diagnostic assessments without rerunning measured traffic"]
fn phase5_assess_evidence() -> Result<()> {
    let out = PathBuf::from(env::var_os("WS_PHASE5_EVIDENCE").ok_or("WS_PHASE5_EVIDENCE required")?).canonicalize()?;
    let assessment_file = PathBuf::from(env::var_os("WS_PHASE5_ASSESSMENT").ok_or("WS_PHASE5_ASSESSMENT required")?).canonicalize()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize()?;
    let assessment: Value = serde_json::from_slice(&fs::read(&assessment_file)?)?;
    let rows = assessment.as_array().ok_or("assessment array")?;
    let campaign: Value = serde_json::from_slice(&fs::read(out.join("campaign.json"))?)?;
    require(campaign["numerical_complete"]==true,"numeric campaign failed or incomplete; assessment cannot promote it")?;
    require(campaign["measurement_hashes"]==measurement_hashes(&out,&root)?,"measurement or validator identity changed")?;
    let profile:Value = serde_json::from_slice(&fs::read(out.join("profile.json"))?)?;
    require(profile==serde_json::from_slice::<Value>(&fs::read(root.join("bench/ws/profiles/local-perf-v2.json"))?)?,"current profile mismatch")?;
    let expected = Profile {raw:profile,cpus:[0,1]}.core()?.into_iter().filter(anchor).map(|c|format!("{c:?}")).collect::<std::collections::BTreeSet<_>>();
    let mut seen = std::collections::BTreeSet::new();
    for entry in fs::read_dir(&out)? {
        let file = entry?.path();
        if file.file_name().is_some_and(|s|s.to_string_lossy().starts_with("limiter-case-")) {
            let report:Value = serde_json::from_slice(&fs::read(file)?)?;
            let workload = report["workload"].as_str().ok_or("limiter workload")?.to_string();
            require(expected.contains(&workload) && seen.insert(workload),"unexpected or duplicated anchor diagnostics")?;
            let matches = rows.iter().filter(|r|r["workload"]==report["workload"]).collect::<Vec<_>>();
            require(matches.len()==1,"unique per-anchor assessment required")?;
            require(report["missing"].as_array().ok_or("limiter missing evidence")?.iter().all(|r|r.as_str().is_some_and(|s|s.contains("missing local evidence-backed limiter assessment"))),"diagnostic collection has unresolved tool or actor failures")?;
            bound_assessment(&report,matches[0],&root)?;
        }
    }
    require(seen==expected,"three distinct expected anchor diagnostic assessments required")?;
    save(&out.join("assessment-result.json"),&json!({"phase_pass":true,"scope":"bounded single-worker qualification","measurement_campaign":"campaign.json","assessment_file":assessment_file,"assessment_hash":checksums(&[assessment_file.clone()])?,"validated_anchors":seen.len()}))
}
