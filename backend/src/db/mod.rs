use centaurus::db::init::Connection;

#[allow(unused)]
pub trait DBTrait {}

impl DBTrait for Connection {}
