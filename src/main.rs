#![feature(path_file_prefix)]

use anyhow::Result;
use lipid::prelude::*;
use polars::prelude::{
    CompatLevel, DataFrame, ParquetWriter, SchemaExt, Series, create_enum_dtype, df,
};
use polars_arrow::array::Utf8ViewArray;
use std::{
    borrow::BorrowMut,
    collections::BTreeMap,
    ffi::OsStr,
    fs::File,
    path::{Path, PathBuf},
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

// [Allow to read and write custom file-level parquet metadata](https://github.com/pola-rs/polars/pull/21806)
//
// [Incompatible with nanoarrow (incorrect Arrow format)](https://github.com/pola-rs/polars/issues/22323)
// https://github.com/apache/arrow-nanoarrow/issues/743
// https://github.com/a pache/arrow-rs/issues/7058

fn main() -> Result<()> {
    // unsafe { std::env::set_var("POLARS_FMT_MAX_COLS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_MAX_ROWS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256") };
    unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "256") };

    // let folder = "D:/git/ippras/hmfa/src/presets/ippras/";
    // let folder = "D:/g/git/ippras/hmfa/src/presets/ippras/";
    // let file = "C70_Control.hmf.ipc";
    // let file = "C70_H2O2.hmf.ipc";
    // let file = "C70_NaCl.hmf.ipc";
    // let file = "H242_-N.0.0.1.hmf.ipc";
    // let file = "H242_-N.0.0.2.hmf.ipc";
    // let file = "H242_-N.0.0.3.hmf.ipc";
    // let file = "H242_-N.hmf.ipc";
    let folder = "D:/g/git/ippras/hmfa/src/presets/10.1021/jf903048p/";
    let file = "MatureMilkFat.ipc";

    let mut path = PathBuf::from(folder);
    path.push(file);
    println!("path: {}", path.display());

    let (mut meta, data) = ipc::polars::read(path)?;
    println!("data: {:?}", data.schema());
    println!("data: {data}");

    // let output = to_parquet(&path)?;
    // parquet::metadata(output, custom_metadata)?;
    // parquet::read(&output)?;

    // let output = ipc_to_ipc(path)?;
    // println!("to_ipc: {output:?}");
    // ipc::polars::read(&output)?;
    // ipc::arrow::read(&output)?;

    Ok(())
}

fn ipc_to_ipc(input: impl AsRef<Path>) -> Result<PathBuf> {
    const EXTENSION: &str = "hmfa.arrow";

    // let fatty_acid = df! {
    //     "FattyAcid" => [
    //         Some(Series::from_iter(C16U0).cast(&BOUND_DATA_TYPE)?),
    //         Some(Series::from_iter(C18U1DC9).cast(&BOUND_DATA_TYPE)?),
    //         Some(Series::from_iter(C18U2DC9DC12).cast(&BOUND_DATA_TYPE)?),
    //         Some(Series::from_iter(C18U3DC9DC12DC15).cast(&BOUND_DATA_TYPE)?),
    //     ],
    // }?;

    let fatty_acid = df! {
        "FattyAcid" => [
            Some(Series::from_iter(C10U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C12U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C14U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C15U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter([Option::<&str>::None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C16U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C16U1DC9).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C16U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C17U0).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C17U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C18U0).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C18U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C18U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C18U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C18U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C20U0).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C20U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C20U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C20U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C20U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C20U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C22U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C22U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C23U0).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C22U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C22U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C22U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C24U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C22U1).cast(&BOUND_DATA_TYPE)?),
            // Some(Series::from_iter(C24U0).cast(&BOUND_DATA_TYPE)?),
        ],
    }?;

    let input = input.as_ref();
    assert!(matches!(input.extension(), Some(extension) if extension == "ipc"));
    let (mut meta, data) = ipc::polars::read(input)?;
    meta = prepare_metadata(meta);
    println!("read data: {:#?}", data.schema());
    println!("read data: {data}");
    let mut data = fatty_acid.hstack(&data.get_columns()[1..])?;
    data.align_chunks(); // !!!
    let output = PathBuf::from(input.file_prefix().unwrap_or(OsStr::new("output")))
        .with_extension(EXTENSION);
    ipc::polars::write(&output, meta, &mut data)?;
    Ok(output)
}

fn prepare_metadata(mut custom_metadata: Metadata) -> Metadata {
    println!("meta: {custom_metadata:#?}");
    println!("CARGO_PKG_AUTHORS: {:?}", env!("CARGO_PKG_AUTHORS"));
    custom_metadata.retain(|_key, value| !value.is_empty());
    custom_metadata.insert("authors".to_owned(), env!("CARGO_PKG_AUTHORS").to_owned());
    println!("meta: {custom_metadata:#?}");
    custom_metadata
}

fn ipc_to_parquet(input: &Path) -> Result<PathBuf> {
    const EXTENSION: &str = "hmfa.parquet";

    let fatty_acid = df! {
        "FattyAcid" => [
            Some(Series::from_iter(C16U0).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U1DC9).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U2DC9DC12).cast(&BOUND_DATA_TYPE)?),
            Some(Series::from_iter(C18U3DC9DC12DC15).cast(&BOUND_DATA_TYPE)?),
        ],
    }?;
    assert!(matches!(input.extension(), Some(extension) if extension == "ipc"));
    let (mut meta, data) = ipc::polars::read(input)?;
    println!("metadata: {meta:#?}");
    meta.retain(|_key, value| !value.is_empty());
    println!("metadata: {meta:#?}");
    println!("data_frame: {data}");
    let mut data = fatty_acid.hstack(&data.get_columns()[1..])?;
    println!("data_frame: {data}");
    data.align_chunks(); // !!!
    let output = PathBuf::from(input.file_prefix().unwrap_or(OsStr::new("output")))
        .with_extension(EXTENSION);
    parquet::write_polars(&output, meta, &mut data)?;
    parquet::read_polars(&output)?;
    Ok(output)
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
