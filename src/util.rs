use byteorder::{LittleEndian, ReadBytesExt};

use std::io::{self, Read};

pub fn try_read_u32<T: Read>(reader: &mut T) -> io::Result<Option<usize>> {
    let mut buffer = [0u8; 4];
    if let Err(err) = reader.read_exact(&mut buffer[..1]) {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        } else {
            return Err(err);
        }
    }
    reader.read_exact(&mut buffer[1..])?;
    let len = (&buffer[..]).read_u32::<LittleEndian>().unwrap();
    Ok(Some(len as usize))
}
