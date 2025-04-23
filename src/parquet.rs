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
    schema::{parser::parse_message_type, printer::print_parquet_metadata},
};
use polars::prelude::{DataFrame, ParquetReader, ParquetWriter, SerReader};
use std::{fs::File, io::stdout, path::Path, sync::Arc};

pub(super) fn metadata(path: impl AsRef<Path>) -> Result<()> {
    let mut metadata = read_metadata_from_local_file(&path)?;
    print_parquet_metadata(&mut stdout(), &metadata);
    metadata = prepare_metadata(metadata);
    print_parquet_metadata(&mut stdout(), &metadata);
    write_metadata_to_local_file(metadata, &path)?;
    Ok(())
}

// https://github.com/apache/arrow-rs/blob/main/parquet/examples/external_metadata.rs

/// Reads the metadata from a file
///
/// This function reads the format written by `write_metadata_to_file`
fn read_metadata_from_local_file(path: impl AsRef<Path>) -> Result<ParquetMetaData> {
    let file = File::open(path)?;
    Ok(ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .parse_and_finish(&file)?)
}

/// writes the metadata to a file
///
/// The data is stored using the same thrift format as the Parquet file metadata
fn write_metadata_to_local_file(metadata: ParquetMetaData, path: impl AsRef<Path>) -> Result<()> {
    let file = File::create(path)?;
    Ok(ParquetMetaDataWriter::new(file, &metadata).finish()?)
}

fn prepare_metadata(metadata: ParquetMetaData) -> ParquetMetaData {
    let file_metadata = metadata.file_metadata();
    let file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        None,
        file_metadata.key_value_metadata().map(ToOwned::to_owned),
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().map(ToOwned::to_owned),
    );
    let mut builder = ParquetMetaDataBuilder::new_from_metadata(metadata);
    builder.builder.build()
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

pub(super) fn read_via_polars(path: impl AsRef<Path>) -> Result<()> {
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

pub(super) fn write_via_polars(
    path: impl AsRef<Path>,
    _meta: Metadata,
    data: &mut DataFrame,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = ParquetWriter::new(file);
    writer.finish(data)?;
    Ok(())
}
