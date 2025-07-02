#![feature(path_file_prefix)]

use anyhow::Result;
use lipid::prelude::*;
use polars::prelude::*;
use polars_arrow::array::Utf8ViewArray;
use std::{
    borrow::BorrowMut,
    collections::BTreeMap,
    ffi::OsStr,
    fs::File,
    num::NonZeroI8,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

// file_named:\g\git\kgv\apache_arrow_ipc\output.hmf.parquet
// created_byPolars
// num_rows4
// num_row_groups1
// format_version1
// encryption_algorithm0
// footer_signing_key_metadata0

pub type Metadata = BTreeMap<String, String>;

// [Allow to read and write custom file-level parquet metadata](https://github.com/pola-rs/polars/pull/21806)
//
// [Incompatible with nanoarrow (incorrect Arrow format)](https://github.com/pola-rs/polars/issues/22323)
// https://github.com/apache/arrow-nanoarrow/issues/743
// https://github.com/apache/arrow-rs/issues/7058

const C15U1: [(Option<Option<NonZeroI8>>, &str); 14] = [
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), U),
];

const C16U2: [(Option<Option<NonZeroI8>>, &str); 15] = [
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), U),
    (Some(None), U),
];

const C17U1: [(Option<Option<NonZeroI8>>, &str); 16] = [
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), U),
];

const C24U1: [(Option<Option<NonZeroI8>>, &str); 23] = [
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), S),
    (Some(None), U),
];

const C22DC4DC7DC10DC13DC16: [&str; 21] = [
    S, S, S, DC, S, S, DC, S, S, DC, S, S, DC, S, S, DC, S, S, S, S, S,
];

fn main() -> Result<()> {
    // unsafe { std::env::set_var("POLARS_FMT_MAX_COLS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_MAX_ROWS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256") };
    unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "256") };

    let input = "file.parquet";
    // parquet::print_metadata(input)?;
    parquet::read(input)?;
    // parquet::metadata(output, custom_metadata)?;
    // parquet::read(input)?;

    // parquet::read_polars(&input)?;
    // parquet::read_polars(&output)?;
    // parquet::write_polars(&output, meta, &mut data)?;

    // let fatty_acids = df! {
    //     "FattyAcid" => [
    //         Some(FattyAcidChunked::try_from(C16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
    //         Some(FattyAcidChunked::try_from(C18DC9)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
    //         Some(FattyAcidChunked::try_from(C18DC9DC12)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
    //         Some(FattyAcidChunked::try_from(C18DC6DC9DC12DC15)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
    //     ]
    // }?;
    // let (meta, mut data) = ipc::polars::read(&path)?;
    // println!("data: {data}");
    // data = fatty_acids.hstack(&data.get_columns()[1..])?;
    // println!("data_frame: {data}");
    // data.align_chunks(); // !!!
    // let output =
    //     PathBuf::from(path.file_prefix().unwrap_or(OsStr::new("output"))).with_extension(EXTENSION);
    // ipc::polars::write(&output, meta, &mut data)?;

    Ok(())
}

fn __main() -> Result<()> {
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
    let folder = "D:/g/git/ippras/_hmfa/src/presets/10.1021/jf903048p/";
    let file = "MatureMilkFat.ipc";

    let mut path = PathBuf::from(folder);
    path.push(file);
    println!("path: {}", path.display());

    let (mut meta, data) = ipc::polars::read(&path)?;
    println!("data: {:?}", data.schema());
    println!("data: {data}");

    // let output = to_parquet(&path)?;
    // parquet::metadata(output, custom_metadata)?;
    // parquet::read(&output)?;

    let output = ipc_to_ipc(path)?;
    println!("to_ipc: {output:?}");
    let (mut meta, mut data) = ipc::polars::read(&output)?;

    // let data_frame = data
    //     .lazy()
    //     .select([
    //         col("FattyAcid").fatty_acid().display(),
    //         col("StereospecificNumber123"),
    //         col("StereospecificNumber2"),
    //     ])
    //     .collect()?;
    // println!("data_frame???: {data_frame}");
    // ipc::arrow::read(&output)?;
    Ok(())
}

// fn main() -> Result<()> {
//     // unsafe { std::env::set_var("POLARS_FMT_MAX_COLS", "256") };
//     unsafe { std::env::set_var("POLARS_FMT_MAX_ROWS", "256") };
//     unsafe { std::env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256") };
//     unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "256") };
//     let (meta, data) = ipc::polars::read("Lunaria rediviva.2024-01-24.1.1.0.utca.ipc")?;
//     println!("data: {data}");
//     let experimental = [
//         0.088, 0.2, 0.0275, 0.0275, 0.0215, 0.0215, 0.09, 0.09, 0.013, 0.0515, 0.0515, 0.018, 0.0,
//         0.0, 0.0, 0.097, 0.0085, 0.0085, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.015, 0.015,
//     ];
//     let data_frame = data
//         .lazy()
//         .with_column(lit(Series::new("Experimental".into(), experimental)))
//         .group_by([col("Label").struct_().field_by_index(1)])
//         .agg([col("Experimental").sum(), col("Value").sum(), col("Label")])
//         .collect()?;
//     println!("data_frame: {data_frame}");
//     // ipc::arrow::read("Lunaria rediviva.2024-01-24.1.1.0.utca.ipc")?;
//     // json_to_ipc("MatureMilk.json")?;
//     Ok(())
// }

// {123} {UUU} {U_3} {S_2U}
// {13=2} {UU=U} {S2=U} {S_2=U} {SU=U}
// {1-2-3} {U-U-U} {U-U-U}

// fn json_to_ipc(input: impl AsRef<Path>) -> Result<PathBuf> {
//     const EXTENSION: &str = "hmfa.arrow";

//     let input = input.as_ref();
//     let mut data = LazyJsonLineReader::new(input).finish()?.collect()?;
//     let output = PathBuf::from(input.file_prefix().unwrap_or(OsStr::new("output")))
//         .with_extension(EXTENSION);
//     ipc::polars::write(&output, Default::default(), &mut data)?;
//     Ok(output)
// }

fn _main() -> Result<()> {
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

    let (mut meta, data) = ipc::polars::read(&path)?;
    println!("data: {:?}", data.schema());
    println!("data: {data}");

    // let output = to_parquet(&path)?;
    // parquet::metadata(output, custom_metadata)?;
    // parquet::read(&output)?;

    let output = ipc_to_ipc(path)?;
    println!("to_ipc: {output:?}");
    let (mut meta, mut data) = ipc::polars::read(&output)?;
    let mut tempfile = std::fs::File::create("tempfile.json").unwrap();
    JsonWriter::new(&mut tempfile)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut data)
        .unwrap();
    let data_frame = data
        .lazy()
        .select([
            col("FattyAcid").fatty_acid().display(),
            col("StereospecificNumber123"),
            col("StereospecificNumber2"),
        ])
        .collect()?;
    // println!("data_frame???: {data_frame}");
    // ipc::arrow::read(&output)?;
    Ok(())
}

fn ipc_to_ipc(input: impl AsRef<Path>) -> Result<PathBuf> {
    const EXTENSION: &str = "hmfa.arrow";

    let fatty_acid = df! {
        "FattyAcid" => [
            Some(FattyAcidChunked::try_from(C10)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C12)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C14)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C15)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C15U1)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C16DC9)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C16U2)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C17)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C17U1)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC9)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC9DC12)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC6DC9DC12)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC9DC12DC15)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C20)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C20DC11)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C20DC11DC14)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C20DC8DC11DC14)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C20DC11DC14DC17)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C20DC5DC8DC11DC14DC17)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C22DC13)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C22DC13DC16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C23)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C22DC7DC10DC13DC16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C22DC7DC10DC13DC16DC19)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C22DC4DC7DC10DC13DC16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C22DC4DC7DC10DC13DC16DC19)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C24)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C24U1)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
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
            Some(FattyAcidChunked::try_from(C16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC9)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC9DC12)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
            Some(FattyAcidChunked::try_from(C18DC9DC12DC15)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
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
//             Some(FattyAcidChunked::try_from(C4)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
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
mod json;
mod parquet;
