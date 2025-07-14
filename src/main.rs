#![feature(path_file_prefix)]
#![feature(path_add_extension)]

use ::parquet::format::KeyValue;
use anyhow::Result;
use fatty_acid_macro::fatty_acid;
use lipid::prelude::*;
use metadata::{AUTHORS, DATE, DESCRIPTION, MetaDataFrame, NAME, VERSION};
use polars::prelude::*;
use polars_arrow::array::Utf8ViewArray;
use std::{
    borrow::BorrowMut,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{File, read_dir},
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

// [Allow to read and write custom file-level parquet metadata](https://github.com/pola-rs/polars/pull/21806)
//
// [Incompatible with nanoarrow (incorrect Arrow format)](https://github.com/pola-rs/polars/issues/22323)
// https://github.com/apache/arrow-nanoarrow/issues/743
// https://github.com/apache/arrow-rs/issues/7058

// {123} {UUU} {U_3} {S_2U}
// {13=2} {UU=U} {S2=U} {S_2=U} {SU=U}
// {1-2-3} {U-U-U} {U-U-U}

pub type Metadata = BTreeMap<String, String>;

const TAG: &str = "TAG";
const MAG: &str = "MAG";
const EXTENSION: &str = "utca.parquet";

fn main() -> Result<()> {
    // unsafe { std::env::set_var("POLARS_FMT_MAX_COLS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_MAX_ROWS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256") };
    unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "256") };

    // let path = Path::new("CONFIG").with_extension(EXTENSION);
    // write_parquet(&path)?;
    // fix_metadata(&path)?;

    let paths = read_dir("_temp")?;
    for path in paths {
        let path = path?.path();
        println!("path: {}", path.display());
        let frame = parquet::read_polars(&path)?;
        let file = File::create(Path::new("_output").join(path.file_name().unwrap()))?;
        frame.write_parquet(file)?;
    }
    Ok(())
}

fn fix_metadata(path: &Path) -> Result<()> {
    let name = "Acer Ginnala";
    let version = "0.0.3";
    let description = "2738, 2774";
    let date = "2025-07-08";
    let output = parquet::set_metadata(
        path,
        "IPPRAS",
        vec![
            KeyValue {
                key: AUTHORS.to_owned(),
                value: Some("Giorgi Vladimirovich Kazakov;Roman Alexandrovich Sidorov".to_owned()),
            },
            KeyValue {
                key: DATE.to_owned(),
                value: Some(date.to_owned()),
            },
            KeyValue {
                key: DESCRIPTION.to_owned(),
                value: Some(description.to_owned()),
            },
            KeyValue {
                key: NAME.to_owned(),
                value: Some(name.to_owned()),
            },
            KeyValue {
                key: VERSION.to_owned(),
                value: Some(version.to_owned()),
            },
        ],
    )?;
    parquet::print_metadata(&output)?;
    parquet::read_polars(&output)?;
    Ok(())
}

// assert!(matches!(input.extension(), Some(extension) if extension == "ipc"));
// let (mut meta, data) = ipc::polars::read(input)?;
// println!("metadata: {meta:#?}");
// meta.retain(|_key, value| !value.is_empty());
// println!("metadata: {meta:#?}");
// println!("data_frame: {data}");
// let mut data = fatty_acid.hstack(&data.get_columns()[1..])?;
// println!("data_frame: {data}");
// data.align_chunks(); // !!!
// let output = PathBuf::from(output).with_extension(EXTENSION);
fn write_parquet(path: &Path) -> Result<()> {
    let mut data = df! {
        FATTY_ACID => Series::from_any_values_and_dtype(FATTY_ACID.into(), &[
            fatty_acid!(C16 { }                             )?,
            fatty_acid!(C18 { }                             )?,
            fatty_acid!(C18 { 6 => DC, 9 => DC, 12 => DC }  )?,
            fatty_acid!(C18 { 9 => DC }                     )?,
            fatty_acid!(C18 { 9 => DC, 12 => DC }           )?,
            fatty_acid!(C18 { 9 => DC, 12 => DC, 15 => DC } )?,
            fatty_acid!(C18 { 11 => DC }                    )?,
            fatty_acid!(C20 { }                             )?,
            fatty_acid!(C20 { 11 => DC }                    )?,
            fatty_acid!(C20 { 11 => DC, 14 => DC }          )?,
            fatty_acid!(C22 { }                             )?,
            fatty_acid!(C22 { 13 => DC }                    )?,
            fatty_acid!(C24 { }                             )?,
            fatty_acid!(C24 { 15 => DC }                    )?,
        ], &data_type!(FATTY_ACID), true)?,
        TAG => [
            Some(7979670.454)    ,
            Some(2295238.017)    ,
            Some(10218630.561)   ,
            Some(39083890.184)   ,
            Some(90724727.520)   ,
            Some(1929464.237)    ,
            Some(3483557.731)    ,
            Some(150650.284)     ,
            Some(8531661.618)    ,
            Some(219058.834)     ,
            Some(523039.524)     ,
            Some(27084176.029)   ,
            Some(249251.243)     ,
            Some(9670803.016)    ,
        ],
        MAG => [
            Some(2169340.502)    ,
            Some(1689235.811)    ,
            Some(27894720.561)   ,
            Some(54572726.906)   ,
            Some(182066477.200)  ,
            Some(2798985.353)    ,
            Some(1752513.118)    ,
            None                 ,
            Some(517806.591)     ,
            None                 ,
            None                 ,
            Some(262069.875)     ,
            None                 ,
            None                 ,
        ]
    }?;
    let meta = Default::default();
    parquet::write_polars(path, meta, &mut data)?;
    parquet::read_polars(path)?;
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
            col("FattyAcid").fatty_acid().format(),
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
        FATTY_ACID => Series::from_any_values_and_dtype(FATTY_ACID.into(), &[
            fatty_acid!(C10 { })?,
            fatty_acid!(C12 { })?,
            fatty_acid!(C14 { })?,
            fatty_acid!(C15 { })?,
            fatty_acid!(C15 { 1 => U })?,
            fatty_acid!(C16 { })?,
            fatty_acid!(C16 { 9 => DC })?,
            fatty_acid!(C16 { 2 => U })?,
            fatty_acid!(C17 { })?,
            fatty_acid!(C17 { 1 => U })?,
            fatty_acid!(C18 { })?,
            fatty_acid!(C18 { 9 => DC })?,
            fatty_acid!(C18 { 9 => DC, 12 => DC })?,
            fatty_acid!(C18 { 6 => DC, 9 => DC, 12 => DC })?,
            fatty_acid!(C18 { 9 => DC, 12 => DC, 15 => DC })?,
            fatty_acid!(C20 { })?,
            fatty_acid!(C20 { 11 => DC })?,
            fatty_acid!(C20 { 11 => DC, 14 => DC })?,
            fatty_acid!(C20 { 8 => DC, 11 => DC, 14 => DC })?,
            fatty_acid!(C20 { 11 => DC, 14 => DC, 17 => DC })?,
            fatty_acid!(C20 { 5 => DC, 8 => DC, 11 => DC, 14 => DC, 17 => DC })?,
            fatty_acid!(C22 { 13 => DC })?,
            fatty_acid!(C22 { 13 => DC, 16 => DC })?,
            fatty_acid!(C23 { })?,
            fatty_acid!(C22 { 7 => DC, 10 => DC, 13 => DC, 16 => DC })?,
            fatty_acid!(C22 { 7 => DC, 10 => DC, 13 => DC, 16 => DC, 19 => DC })?,
            fatty_acid!(C22 { 4 => DC, 7 => DC, 10 => DC, 13 => DC, 16 => DC })?,
            fatty_acid!(C22 { 4 => DC, 7 => DC, 10 => DC, 13 => DC, 16 => DC, 19 => DC })?,
            fatty_acid!(C24 { })?,
            fatty_acid!(C24 { 1 => U })?,
        ], &data_type!(FATTY_ACID), true)?,
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
        FATTY_ACID => Series::from_any_values_and_dtype(FATTY_ACID.into(), &[
            fatty_acid!(C16 { })?,
            fatty_acid!(C17 { })?,
            fatty_acid!(C17 { 1 => U })?,
            fatty_acid!(C18 { })?,
            fatty_acid!(C18 { 9 => DC })?,
            fatty_acid!(C18 { 9 => DC, 12 => DC })?,
            fatty_acid!(C18 { 6 => DC, 9 => DC, 12 => DC })?,
            fatty_acid!(C18 { 9 => DC, 12 => DC, 15 => DC })?,
            fatty_acid!(C20 { })?,
            fatty_acid!(C20 { 11 => DC })?,
            fatty_acid!(C20 { 11 => DC, 14 => DC })?,
            fatty_acid!(C20 { 8 => DC, 11 => DC, 14 => DC })?,
            fatty_acid!(C20 { 11 => DC, 14 => DC, 17 => DC })?,
            fatty_acid!(C20 { 5 => DC, 8 => DC, 11 => DC, 14 => DC, 17 => DC })?,
            fatty_acid!(C22 { 13 => DC })?,
            fatty_acid!(C22 { 13 => DC, 16 => DC })?,
            fatty_acid!(C23 { })?,
            fatty_acid!(C22 { 7 => DC, 10 => DC, 13 => DC, 16 => DC })?,
            fatty_acid!(C22 { 7 => DC, 10 => DC, 13 => DC, 16 => DC, 19 => DC })?,
            fatty_acid!(C22 { 4 => DC, 7 => DC, 10 => DC, 13 => DC, 16 => DC })?,
            fatty_acid!(C22 { 4 => DC, 7 => DC, 10 => DC, 13 => DC, 16 => DC, 19 => DC })?,
            fatty_acid!(C24 { })?,
            fatty_acid!(C24 { 1 => U })?,
        ], &data_type!(FATTY_ACID), true)?,
        // "FattyAcid" => [
        //     Some(FattyAcidChunked::try_from(C16)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
        //     Some(FattyAcidChunked::try_from(C18DC9)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
        //     Some(FattyAcidChunked::try_from(C18DC9DC12)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
        //     Some(FattyAcidChunked::try_from(C18DC9DC12DC15)?.into_struct(PlSmallStr::EMPTY)?.into_series()),
        // ],
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
