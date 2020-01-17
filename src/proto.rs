use byte_string::ByteStr;

use std::fmt::{
    self,
    Display,
    Formatter,
};

tonic::include_proto!("ray");

impl Display for SetRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SetRequest {{key: {:?}, value: {:?}}}",
            ByteStr::new(&self.key),
            ByteStr::new(&self.value),
        )
    }
}

impl Display for SetReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SetOk")
    }
}

impl Display for GetRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "GetRequest {{key: {:?}}}",
            ByteStr::new(&self.key),
        )
    }
}

impl Display for GetReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "GetReply {{value: {:?}}}",
            ByteStr::new(&self.value),
        )
    }
}
