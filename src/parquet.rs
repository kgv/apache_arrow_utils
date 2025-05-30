use crate::Metadata;
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
use polars::prelude::{DataFrame, ParquetReader, ParquetWriter, SerReader};
use polars_arrow::array::ViewType;
use std::{fs::File, io::stdout, path::Path, sync::Arc};

// https://github.com/apache/arrow-rs/blob/main/parquet/examples/external_metadata.rs
pub(super) fn metadata(path: impl AsRef<Path>, custom_metadata: Metadata) -> Result<()> {
    print_metadata(&path)?;
    let mut metadata = read_metadata(&path)?;
    // metadata = prepare_metadata(metadata, custom_metadata);
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
    _meta: Metadata,
    data: &mut DataFrame,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = ParquetWriter::new(file);
    writer.finish(data)?;
    Ok(())
}
