use anyhow::Result;
use fatty_acid_macro::fatty_acid;
use lipid::prelude::*;
use maplit::btreemap;
use metadata::{AUTHORS, DATE, DEFAULT_DATE, DESCRIPTION, NAME, VERSION, polars::MetaDataFrame};
use polars::prelude::*;
use ron::{extensions::Extensions, ser::PrettyConfig};
use std::{
    borrow::BorrowMut,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{File, read_dir},
    io::Write as _,
    num::NonZeroI8,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};
use walkdir::WalkDir;

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

// {1, 2, 3}
// {123} {UUU} {U3} {S2U}
// {13=2} {UU=U} {S2=U} {S_2=U} {SU=U}
// {1-2-3} {U-U-U} {U-U-U}

pub type Metadata = BTreeMap<String, String>;

const TAG: &str = "Triacylglycerol";
const MAG: &str = "Monoacylglycerol2";
const EXTENSION: &str = "utca.ron";

fn main() -> Result<()> {
    unsafe { std::env::set_var("POLARS_FMT_MAX_ROWS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256") };
    unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "256") };

    christie()?;
    // create_new()?;
    Ok(())
}

fn christie() -> Result<()> {
    // let path = "D:/git/kgv/apache_arrow_utils/Christie.ipc";
    // let file = File::open(path).unwrap();
    // let mut reader = IpcReader::new(file);
    // let meta = reader.custom_metadata().unwrap();
    // println!("meta: {meta:?}");
    // let data = reader.finish().unwrap();
    // println!("data: {data}");

    let name = "Christie";
    let authors = "Giorgi Vladimirovich Kazakov,Roman Alexandrovich Sidorov";
    let date = DEFAULT_DATE;
    let description = "";
    let version = "0.0.0";
    let meta = metadata::Metadata(btreemap! {
        AUTHORS.to_owned() => authors.to_owned(),
        DATE.to_owned() => date.to_owned(),
        DESCRIPTION.to_owned() => description.to_owned(),
        NAME.to_owned() => name.to_owned(),
        VERSION.to_owned() => version.to_owned(),
    });
    println!("meta: {meta:?}");
    let data = df! {
        FATTY_ACID => [
            fatty_acid!(C10 {})?,
            fatty_acid!(C11 {})?,
            fatty_acid!(C12 {})?,
            fatty_acid!(C13 {})?,
            fatty_acid!(C14 {})?,
            fatty_acid!(C15 {})?,
            fatty_acid!(C15 {9 => C})?,
            fatty_acid!(C16 {})?,
            fatty_acid!(C16 {9 => C})?,
            fatty_acid!(C18 {7 => C})?,
            fatty_acid!(C18 {9 => C})?,
            fatty_acid!(C18 {8 => C, 10 => C})?,
            fatty_acid!(C18 {9 => C, 12 => C})?,
            fatty_acid!(C18 {6 => C, 9 => C, 12 => C})?,
            fatty_acid!(C18 {9 => C, 12 => C, 15 => C})?,
            fatty_acid!(C20 {})?,
            fatty_acid!(C20 {11 => C})?,
            fatty_acid!(C20 {11 => C, 14 => C})?,
            fatty_acid!(C21 {})?,
            fatty_acid!(C20 {5 => C, 8 => C, 11 => C, 14 => C})?,
            fatty_acid!(C20 {5 => C, 8 => C, 11 => C, 14 => C, 17 => C})?,
            fatty_acid!(C22 {})?,
            fatty_acid!(C22 {13 => C})?,
            fatty_acid!(C23 {})?,
            fatty_acid!(C24 {})?,
            fatty_acid!(C22 {4 => C, 7 => C, 10 => C, 13 => C, 16 => C, 19 => C})?,
            fatty_acid!(C24 {15 => C})?,
        ],
        "Factor" => [
            1.36,
            1.319,
            1.278,
            1.238,
            1.196,
            1.158,
            1.158,
            1.109,
            1.116,
            0.997,
            0.997,
            1.112,
            1.112,
            1.265,
            1.265,
            0.987,
            0.976,
            1.3,
            0.99,
            1.385,
            1.247,
            0.958,
            0.932,
            0.933,
            0.922,
            1.494,
            0.888,
        ],
    }?;
    println!("data: {data}");

    let frame = MetaDataFrame::new(meta, data);
    let serialized = ron::ser::to_string_pretty(
        &frame,
        PrettyConfig::new().extensions(Extensions::UNWRAP_NEWTYPES),
    )?;
    let mut file = File::create("D:/git/kgv/apache_arrow_utils/Christie.ron").unwrap();
    file.write_all(serialized.as_bytes())?;
    Ok(())
}

// fatty_acid!(C10 {});
// fatty_acid!(C11 {});
// fatty_acid!(C12 {});
// fatty_acid!(C13 {});
// fatty_acid!(C14 {});
// fatty_acid!(C15 {});
// fatty_acid!(C15 {9 => C});
// fatty_acid!(C16 {});
// fatty_acid!(C16 {9 => C});
// fatty_acid!(C18 {7 => C});
// fatty_acid!(C18 {9 => C});
// fatty_acid!(C18 {8 => C, 10 => C});
// fatty_acid!(C18 {9 => C, 12 => C});
// fatty_acid!(C18 {6 => C, 9 => C, 12 => C});
// fatty_acid!(C18 {9 => C, 12 => C, 15 => C});
// fatty_acid!(C20 {});
// fatty_acid!(C20 {11 => C});
// fatty_acid!(C20 {11 => C, 14 => C});
// fatty_acid!(C21 {});
// fatty_acid!(C20 {5 => C, 8 => C, 11 => C, 14 => C});
// fatty_acid!(C20 {5 => C, 8 => C, 11 => C, 14 => C, 17 => C});
// fatty_acid!(C22 {});
// fatty_acid!(C22 {13 => C});
// fatty_acid!(C23 {});
// fatty_acid!(C24 {});
// fatty_acid!(C22 {4 => C, 7 => C, 10 => C, 13 => C, 16 => C, 19 => C});
// fatty_acid!(C24 {15 => C});

// | #   | Идентификатор            |
// | --- | ------------------------ |
// | 1   | К-2233, Прогресс, Россия |
// | 2   | К-2699, Прогресс, Россия |
// | 3   | К-3599, RIL-130, Франция |
// | 4   | К-3675, ВИР 839, Россия  |
// | 5   | К-3384, ВИР 584, Россия  |
// | 6   | К-3714, ВИР 172Б, Россия |
// | 7   | К-2776, ВИР 136, Россия  |
fn create_new() -> Result<()> {
    let name = "К-3599";
    let authors = "Giorgi Vladimirovich Kazakov,Roman Alexandrovich Sidorov";
    let date = "2025-09-03";
    let description = "К-3599, RIL-130, Франция\n#2893, #3176";
    let version = "0.0.1";

    let data = df! {
                    "Label" => [
    "Methyl tetradecanoate",
    "Hexadecanoic acid, methyl ester",
    "9-Hexadecenoic acid, methyl ester, (Z)-",
    "(Z)-Methyl hexadec-11-enoate",
    "Methyl stearate",
    "9-Octadecenoic acid (Z)-, methyl ester",
    "11-Octadecenoic acid, methyl ester, (Z)-",
    "9,12-Octadecadienoic acid (Z,Z)-, methyl ester",
    "Eicosanoic acid, methyl ester",
    "9,12,15-Octadecatrienoic acid, methyl ester, (Z,Z,Z)-",
    "cis-Methyl 11-eicosenoate",
    "Docosanoic acid, methyl ester",
    "Tetracosanoic acid, methyl ester",
                    ],
                    FATTY_ACID => Series::from_any_values_and_dtype(FATTY_ACID.into(), &[
                        fatty_acid!(C14 {})?,
                        fatty_acid!(C16 {})?,
                        fatty_acid!(C16 {9 => C})?,
                        fatty_acid!(C16 {11 => C})?,
                        fatty_acid!(C18 {})?,
                        fatty_acid!(C18 {9 => C})?,
                        fatty_acid!(C18 {11 => C})?,
                        fatty_acid!(C18 {9 => C, 12 => C})?,
                        fatty_acid!(C20 {})?,
                        fatty_acid!(C18 {9 => C, 12 => C, 15 => C})?,
                        fatty_acid!(C20 {11 => C})?,
                        fatty_acid!(C22 {})?,
                        fatty_acid!(C24 {})?,
                    ], &data_type!(FATTY_ACID), true)?,
                    STEREOSPECIFIC_NUMBERS123=> [
    215454.396,
    42504128.037,
    59892.626,
    257142.969,
    42300327.588,
    299467874.231,
    5189196.189,
    278334952.567,
    1704389.894,
    163763.513,
    468005.210,
    3649205.210,
    449996.838,
                    ],
                    STEREOSPECIFIC_NUMBERS2 => [
    0.0,
    43111.573,
    0.0,
    0.0,
    0.0,
    17984537.307,
    0.0,
    17315664.590,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
                    ],
                    // STEREOSPECIFIC_NUMBERS2 => df!{
                    //     "RetentionTime" => [
                    //         Some(10.071),
                    //         None,
                    //         Some(32.783),
                    //     ],
                    //     "PeakArea" => [
                    //         77949.0,
                    //     ]
                    // }?.into_struct(PlSmallStr::EMPTY),
                }?;
    let meta = metadata::Metadata(btreemap! {
        AUTHORS.to_owned() => authors.to_owned(),
        DATE.to_owned() => date.to_owned(),
        DESCRIPTION.to_owned() => description.to_owned(),
        NAME.to_owned() => name.to_owned(),
        VERSION.to_owned() => version.to_owned(),
    });
    let path = Path::new("_output")
        .join(meta.format(".").to_string())
        .with_added_extension(EXTENSION);
    let mut file = File::create(&path)?;
    let frame = MetaDataFrame::new(meta, data);
    let serialized = ron::ser::to_string_pretty(
        &frame,
        PrettyConfig::new().extensions(Extensions::UNWRAP_NEWTYPES),
    )?;
    file.write_all(serialized.as_bytes())?;
    // MetaDataFrame::new(meta, &mut data).write_parquet(file)?;
    Ok(())
}
