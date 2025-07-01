use anyhow::Result;
use lipid::prelude::*;
use polars::prelude::*;
use std::{fs::File, path::Path};

// pub(crate) fn read(path: impl AsRef<Path>) -> Result<()> {
//     let data_frame = LazyJsonLineReader::new(path)
//         .finish()?
//         .select([
//             col("FattyAcid")
//                 .cast(DataType::List(Box::new(FATTY_ACID_DATA_TYPE.clone())))
//                 .fatty_acid()
//                 .display(),
//             col("StereospecificNumber123"),
//             col("StereospecificNumber2"),
//         ])
//         .with_row_index("Index", None)
//         .collect()?;
//     println!("{data_frame}");
//     Ok(())
// }
