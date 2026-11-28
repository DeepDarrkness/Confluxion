#[path = "schema/native.rs"] mod native;

#[test]
fn native_bsatn() { native::run(bend_codec::Format::Bsatn); }
#[test]
fn native_raw_be() { native::run(bend_codec::Format::RawBe); }
