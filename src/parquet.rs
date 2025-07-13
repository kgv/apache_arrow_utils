use crate::{EXTENSION, Metadata};
use anyhow::Result;
use arrow::{
    array::{
        Float64Array, Int64Array, RecordBatch, RecordBatchReader, TimestampMillisecondArray,
        UInt16Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::reader::FileReader,
    util::pretty::print_batches,
};
use metadata::{AUTHORS, DATE, NAME, VERSION};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::{
        metadata::{
            FileMetaData, ParquetMetaData, ParquetMetaDataBuilder, ParquetMetaDataReader,
            ParquetMetaDataWriter,
        },
        properties::WriterProperties,
        reader::{FileReader as _, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    format::KeyValue,
    schema::{parser::parse_message_type, printer::print_parquet_metadata},
};
use polars::prelude::{DataFrame, KeyValueMetadata, ParquetReader, ParquetWriter, SerReader};
use polars_arrow::array::ViewType;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::format,
    fs::File,
    io::stdout,
    path::{Path, PathBuf},
    sync::Arc,
};

// created by: Polars
// `<application> version <application version> (build <application build hash>)`
// `parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)`
pub(super) fn set_metadata(
    path: impl AsRef<Path>,
    created_by: &str,
    mut key_values: Vec<KeyValue>,
) -> Result<PathBuf> {
    let input = path.as_ref();
    let name = key_values
        .iter()
        .find_map(|key_value| (key_value.key == NAME).then_some(key_value.value.as_deref()))
        .flatten()
        .unwrap_or_default();
    let date = key_values
        .iter()
        .find_map(|key_value| (key_value.key == DATE).then_some(key_value.value.as_deref()))
        .flatten()
        .unwrap_or_default();
    let version = key_values
        .iter()
        .find_map(|key_value| (key_value.key == VERSION).then_some(key_value.value.as_deref()))
        .flatten()
        .unwrap_or_default();
    let output = PathBuf::from(format!("{name}.{date}.{version}.{EXTENSION}"));
    // Metadata
    let reader = SerializedFileReader::new(File::open(input)?)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    println!(
        "created_by: {:?} -> {}",
        file_metadata.created_by(),
        created_by,
    );
    let key_value_metadata = file_metadata
        .key_value_metadata()
        .map(|key_value_metadata| {
            print_metadata_key_value(key_value_metadata, &key_values, NAME);
            print_metadata_key_value(key_value_metadata, &key_values, AUTHORS);
            print_metadata_key_value(key_value_metadata, &key_values, VERSION);
            key_values.append(&mut key_value_metadata.clone());
            key_values.sort_by(|left, right| left.key.cmp(&right.key));
            key_values.dedup_by(|left, right| left.key == right.key);
            key_values
        });
    // Dataframe
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?.build()?;
    let writer_properties = WriterProperties::builder()
        .set_created_by(created_by.to_owned())
        .set_key_value_metadata(key_value_metadata)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&output)?,
        reader.schema(),
        Some(writer_properties),
    )?;
    for maybe_batch in reader {
        let batch = maybe_batch.expect("reading batch");
        writer.write(&batch).expect("writing data");
    }
    // for row in iter {
    //     println!("{}", row?);
    //     writer.write(&batch).expect("writing data");
    // }
    writer.close()?;
    Ok(output)
}

fn print_metadata_key_value(from: &[KeyValue], to: &[KeyValue], key: &str) {
    println!(
        "{key}: {:?} => {:?}",
        from.iter().find(|key_value| key_value.key == key),
        to.iter()
            .find_map(|key_value| (key_value.key == key).then_some(key_value.value.as_ref()))
            .flatten(),
    );
}

pub(super) fn read(path: impl AsRef<Path>) -> Result<()> {
    let input = path.as_ref();
    let reader = SerializedFileReader::new(File::open(input)?)?;
    let metadata = reader.metadata();
    print_parquet_metadata(&mut stdout(), metadata);
    let key_value_metadata = metadata
        .file_metadata()
        .key_value_metadata()
        .map(ToOwned::to_owned);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?.build()?;

    let output = "OUTPUT.parquet";
    let writer_properties = WriterProperties::builder()
        .set_created_by("IPPRAS".to_owned())
        .set_key_value_metadata(key_value_metadata)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(output)?,
        reader.schema(),
        Some(writer_properties),
    )?;
    for maybe_batch in reader {
        let batch = maybe_batch.expect("reading batch");
        writer.write(&batch).expect("writing data");
    }
    // for row in iter {
    //     println!("{}", row?);
    //     writer.write(&batch).expect("writing data");
    // }
    writer.close()?;
    Ok(())
}

// https://github.com/apache/arrow-rs/blob/main/parquet/examples/external_metadata.rs
pub(super) fn metadata(path: impl AsRef<Path>, custom_metadata: Metadata) -> Result<()> {
    print_metadata(&path)?;
    let mut metadata = read_metadata(&path)?;
    metadata = prepare_metadata(metadata, custom_metadata);
    println!("metadata: {metadata:?}");
    write_metadata(metadata, "TEST2.parquet")?;
    print_metadata(&path)?;
    Ok(())
}

pub(super) fn print_metadata(path: impl AsRef<Path>) -> Result<()> {
    let metadata = read_metadata(&path)?;
    println!("file_metadata: {:#?}", metadata.file_metadata());
    print_parquet_metadata(&mut stdout(), &metadata);
    Ok(())
}

/// Reads the metadata from a file
///
/// This function reads the format written by `write_metadata_to_file`
fn read_metadata(path: impl AsRef<Path>) -> Result<ParquetMetaData> {
    let file = File::open(path)?;
    Ok(ParquetMetaDataReader::new()
        // .with_page_indexes(true)
        // .with_column_indexes(true)
        // .with_offset_indexes(true)
        .parse_and_finish(&file)?)
}

/// writes the metadata to a file
///
/// The data is stored using the same thrift format as the Parquet file metadata
fn write_metadata(metadata: ParquetMetaData, path: impl AsRef<Path>) -> Result<()> {
    let file = File::create(path)?;
    Ok(ParquetMetaDataWriter::new(file, &metadata).finish()?)
}

fn prepare_metadata(metadata: ParquetMetaData, mut custom_metadata: Metadata) -> ParquetMetaData {
    // let created_by = format!(
    //     "{} version {}",
    //     env!("CARGO_PKG_NAME"),
    //     env!("CARGO_PKG_VERSION"),
    // );
    let file_metadata = metadata.file_metadata();
    // let mut key_value_metadata = file_metadata
    //     .key_value_metadata()
    //     .cloned()
    //     .unwrap_or_default();
    // for (key, value) in key_value.into_iter() {
    //     if !value.is_empty() {
    //         key_value_metadata.push(KeyValue::new(key, Some(value)));
    //     }
    // }
    // let file_metadata = FileMetaData::new(
    //     file_metadata.version(),
    //     file_metadata.num_rows(),
    //     Some(created_by),
    //     Some(key_value_metadata),
    //     file_metadata.schema_descr_ptr(),
    //     file_metadata.column_orders().cloned(),
    // );
    let mut builder = metadata.into_builder();
    // let builder = ParquetMetaDataBuilder::new(file_metadata.clone());
    // .set_row_groups(builder.take_row_groups());
    // .set_column_index(builder.take_column_index())
    // .set_offset_index(builder.take_offset_index());
    builder.build()
}

fn process_metadata(metadata: &ParquetMetaData) -> ParquetMetaData {
    let file_metadata = metadata.file_metadata();
    let file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        None,
        file_metadata.key_value_metadata().map(ToOwned::to_owned),
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().map(ToOwned::to_owned),
    );
    ParquetMetaData::new(file_metadata, metadata.row_groups().to_vec())
}

// fn write_parquet(
//     path: impl AsRef<Path>,
//     mut frame: MetaDataFrame<Metadata, DataFrame>,
// ) -> Result<()> {
//     // frame.data.schema()
//     let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
//     // let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
//     let batch = RecordBatch::try_new(
//         schema.clone(),
//         vec![
//             Arc::new(UInt64Array::from_iter_values(identifier, count)),
//             Arc::new(UInt16Array::from_value(value, count)),
//             Arc::new(TimestampMillisecondArray::from_value(
//                 date_time.timestamp_millis(),
//                 count,
//             )),
//         ],
//     )?;
//     let file = File::create(path)?;
//     let mut writer = ParquetWriter::try_new(file, schema.clone(), None)?;
//     // writer.metadata(self.meta);
//     writer.finish(&mut frame.data)?;
//     Ok(())
// }

pub(super) fn read_polars(path: impl AsRef<Path>) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = ParquetReader::new(file).set_rechunk(true);
    let meta = reader.get_metadata()?;
    println!("meta: {meta:#?}");
    let custom_meta = meta.key_value_metadata();
    println!("custom_meta: {custom_meta:#?}");
    let data = reader.finish()?;
    println!("data: {data}");
    Ok(())
}

pub(super) fn write_polars(
    path: impl AsRef<Path>,
    meta: Metadata,
    data: &mut DataFrame,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = ParquetWriter::new(file);
    // writer.with_key_value_metadata(Some(KeyValueMetadata::Static(meta.into())));
    writer.finish(data)?;
    Ok(())
}
