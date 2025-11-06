use anyhow::Result;
use fatty_acid_macro::fatty_acid;
use lipid::prelude::*;
use maplit::btreemap;
use metadata::{AUTHORS, DATE, DESCRIPTION, NAME, VERSION, polars::MetaDataFrame};
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

    create_new()?;
    Ok(())
}

// | #   | Идентификатор            |
// | --- | ------------------------ |
// | 1   | К-2233, Прогресс, Россия |
// | 2   | К-2699, Прогресс, Россия |
// | 3   | К-3599, RIL-130, Франция |
// | 4   | К-3675, ВИР 839, Россия  |
// | 5   | К-3384, ВИР 584, Россия  |
// | 6   | К-3714, ВИР 172Б, Россия |
// | 7   | К-2776, ВИР 136, Россия  |

// "Palmitic",
// "Stearic",
// "Oleic",
// "Asclepic",
// "Linoleic",
// "Arachidic",
// "Behenic",

// "Hexadecanoic acid, methyl ester",
// "Methyl stearate",
// "9-Octadecenoic acid (Z)-, methyl ester",
// "11-Octadecenoic acid, methyl ester, (Z)-",
// "9,12-Octadecadienoic acid (Z,Z)-, methyl ester",
// "Eicosanoic acid, methyl ester",
// "Docosanoic acid, methyl ester",

fn create_new() -> Result<()> {
    let name = "К-3714";
    let date = "2025-10-31";
    let version = "0.0.2";
    let description = "К-3714, ВИР 172Б, Россия\n#2904, #3189";
    let authors = "Giorgi Vladimirovich Kazakov,Roman Alexandrovich Sidorov";

    let data = df! {
                    "Label" => [
    "Palmitic",
    "Stearic",
    "Oleic",
    "Asclepic",
    "Linoleic",
    "Arachidic",
    "Behenic",
                    ],
                    FATTY_ACID => Series::from_any_values_and_dtype(FATTY_ACID.into(), &[
                        fatty_acid!(C16 {})?,
                        fatty_acid!(C18 {})?,
                        fatty_acid!(C18 {9 => C})?,
                        fatty_acid!(C18 {11 => C})?,
                        fatty_acid!(C18 {9 => C, 12 => C})?,
                        fatty_acid!(C20 {})?,
                        fatty_acid!(C22 {})?,
                    ], &data_type!(FATTY_ACID), true)?,
                    STEREOSPECIFIC_NUMBERS123=> [
    35981173.982,
    19475043.577,
    213853841.431,
    5093013.983,
    286270506.018,
    965778.126,
    1857847.016,
                    ],
                    STEREOSPECIFIC_NUMBERS2 => [
    135655.710,
    0.0,
    5857044.707,
    0.0,
    12944265.127,
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
