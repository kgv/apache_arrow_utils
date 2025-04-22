use anyhow::Result;
use lipid::prelude::*;
use polars::prelude::{
    CompatLevel, DataFrame, ParquetWriter, SchemaExt, Series, create_enum_dtype, df,
};
use polars_arrow::array::Utf8ViewArray;
use std::{
    borrow::BorrowMut,
    collections::BTreeMap,
    fs::File,
    path::Path,
    sync::{Arc, LazyLock},
};
// use parquet::arrow::{DataType, Field, Schema};

// file_named:\g\git\kgv\apache_arrow_ipc\output.hmf.parquet
// created_byPolars
// num_rows4
// num_row_groups1
// format_version1
// encryption_algorithm0
// footer_signing_key_metadata0

pub type Metadata = BTreeMap<String, String>;

/// Array of bond identifiers.
pub const IDENTIFIERS: [&str; 10] = [S, D, DC, DT, T, TC, TT, U, UC, UT];

/// The bond data type.
pub const BOUND_DATA_TYPE: LazyLock<polars::prelude::DataType> = LazyLock::new(|| {
    let categories = Utf8ViewArray::from_slice_values(IDENTIFIERS);
    create_enum_dtype(categories)
});

// https://github.com/pola-rs/polars/issues/22323
// https://github.com/apache/arrow-nanoarrow/issues/743
// https://github.com/apache/arrow-rs/issues/7058
fn main() -> Result<()> {
    // to_ipc()
    to_parquet()
}

fn to_ipc() -> Result<()> {
    let fatty_acid = df! {
        "FattyAcid" => [
            Some(Series::from_iter(C16U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U1DC9).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U2DC9DC12).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U3DC9DC12DC15).cast(&BOUND_DATA_TYPE)?),
        ],
    }?;
    let path = Path::new("D:/g/git/ippras/hmfa/src/presets/ippras/C70_Control.hmf.ipc");
    assert!(matches!(path.extension(), Some(extension) if extension == "ipc"));
    let (mut meta, data) = ipc::read_polars(path)?;
    println!("metadata: {meta:#?}");
    meta.retain(|_key, value| !value.is_empty());
    println!("metadata: {meta:#?}");
    println!("data_frame: {:#?}", data.schema());
    println!("data_frame: {data}");
    let mut data = fatty_acid.hstack(&data.get_columns()[1..])?;
    // data = data.select_by_range(1..)?;
    println!("data_frame: {data}");
    data.align_chunks(); // !!!
    ipc::write_polars("output.hmfa.arrow", meta, &mut data)?;
    ipc::read_polars("output.hmfa.arrow")?;
    ipc::read("output.hmfa.arrow")?;
    Ok(())
}

fn to_parquet() -> Result<()> {
    let fatty_acid = df! {
        "FattyAcid" => [
            Some(Series::from_iter(C16U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U1DC9).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U2DC9DC12).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U3DC9DC12DC15).cast(&BOUND_DATA_TYPE)?),
        ],
    }?;
    let path = Path::new("D:/g/git/ippras/hmfa/src/presets/ippras/C70_Control.hmf.ipc");
    assert!(matches!(path.extension(), Some(extension) if extension == "ipc"));
    let (mut meta, data) = ipc::read_polars(path)?;
    println!("metadata: {meta:#?}");
    meta.retain(|_key, value| !value.is_empty());
    println!("metadata: {meta:#?}");
    println!("data_frame: {data}");
    let mut data = fatty_acid.hstack(&data.get_columns()[1..])?;
    println!("data_frame: {data}");
    data.align_chunks(); // !!!
    parquet::write_via_polars("output.hmfa.parquet", meta, &mut data)?;
    parquet::read_via_polars("output.hmfa.parquet")?;
    parquet::read("output.hmfa.parquet")?;
    Ok(())
}

// fn ipc_to_parquet() -> Result<()> {
//     let names = df! {
//         "FattyAcid" => [
//             Some(Series::from_iter(C4U0).cast(&BOUND_DATA_TYPE)?),
//         ],
//     }?;
//     let path = Path::new("presets/ippras/C70_Control.hmf.ipc");
//     assert!(matches!(path.extension(), Some(extension) if extension == "ipc"));
//     let MetaDataFrame { meta, mut data } = read_frame(path)?;
//     println!("metadata: {meta:#?}");
//     println!("data_frame: {:#?}", data.schema());
//     println!("data_frame: {data}");
//     data = data
//         .lazy()
//         .select([
//             col("FattyAcid").struct_().field_by_name("Carbons"),
//             col("StereospecificNumber123"),
//             col("StereospecificNumber2"),
//         ])
//         // .with_columns([
//         //     lit(NULL).cast(DataType::Float64).alias("Triacylglycerol"),
//         //     lit(NULL).cast(DataType::Float64).alias("Monoacylglycerol2"),
//         // ])
//         .collect()?;
//     println!("data_frame: {data}");
//     write_parquet("output.hmf.parquet", MetaDataFrame { meta, data })?;
//     Ok(())
// }

// fn write_parquet<D: BorrowMut<DataFrame>>(
//     path: impl AsRef<Path>,
//     mut frame: MetaDataFrame<Metadata, D>,
// ) -> Result<()> {
//     let file = File::create(path)?;
//     // let message_type = "message schema {
//     //     REQUIRED INT32 FattyAcid;
//     //     REQUIRED INT32FLOAT64 StereospecificNumber123;
//     //     REQUIRED FLOAT64 StereospecificNumber2;
//     // }";
//     // let schema = Arc::new(parse_message_type(message_type)?);
//     let schema = Arc::new(Schema::new(vec![
//         Field::new_list(
//             "FattyAcid",
//             Field::new_list_field(DataType::Float64, false),
//             false,
//         ),
//         Field::new("StereospecificNumber123", DataType::Float64, false),
//         Field::new("StereospecificNumber2", DataType::Float64, false),
//     ]));
//     let mut writer = ArrowWriter::try_new(file, schema, None)?;
//     let data_frame = frame.data.borrow_mut();
//     for batch in data_frame.iter_chunks(CompatLevel::newest(), true) {
//         writer.write(batch.into())?;
//     }
//     // let mut writer = SerializedFileWriter::new(file, schema, Default::default())?;
//     // let mut row_group_writer = writer.next_row_group()?;
//     // let mut writer = ParquetWriter::new(file);
//     // writer.metadata(self.meta);
//     // writer.finish(frame.data.borrow_mut())?;
//     Ok(())
// }

mod ipc;
mod parquet;
