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
        metadata::{FileMetaData, ParquetMetaData},
        properties::WriterProperties,
        reader::{FileReader as _, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    schema::{parser::parse_message_type, printer::print_parquet_metadata},
};
use polars::prelude::{DataFrame, ParquetReader, ParquetWriter, SerReader};
use std::{fs::File, io::stdout, path::Path, sync::Arc};

pub(super) fn read(path: impl AsRef<Path>) -> Result<()> {
    let input = path.as_ref();
    let output = "OUTPUT.parquet";
    let reader = SerializedFileReader::new(File::open(input)?)?;
    let metadata = reader.metadata();
    print_parquet_metadata(&mut stdout(), metadata);
    for row in reader {
        println!("{}", row?);
    }
    // let message_type = "
    //     message schema {
    //         OPTIONAL group FattyAcid (LIST) {
    //             REPEATED group list {
    //                 OPTIONAL BYTE_ARRAY element (STRING);
    //             }
    //         }
    //         OPTIONAL DOUBLE StereospecificNumber123;
    //         OPTIONAL DOUBLE StereospecificNumber2;
    //     }
    // ";
    // let schema = Arc::new(parse_message_type(message_type)?);
    // let mut writer = SerializedFileWriter::new(File::create(output)?, schema, Default::default())?;
    // let writer_properties = WriterProperties::builder()
    //     .set_key_value_metadata(
    //         metadata
    //             .file_metadata()
    //             .key_value_metadata()
    //             .map(ToOwned::to_owned),
    //     )
    //     .build();
    // for i in 0..metadata.num_row_groups() {
    //     let row_group_reader = reader.get_row_group(i)?;
    //     let row_group_metadata = metadata.row_group(i);
    //     let mut row_group_writer = writer.next_row_group()?;
    //     // if let Some(mut column_writer) = row_group_writer.next_column()? {
    //     //     let columns = row_group_reader.get_column_reader().into_columns();
    //     //     column_writer.typed().write_batch(&columns.1, None, None)?;
    //     // }
    //     // column_writer.typed().write_batch(
    //     //     &row_group_reader.get_column_reader(0)?,
    //     //     None,
    //     //     None,
    //     // )?;
    //     // let columns = row_group_reader.get_column_reader().into_columns();
    //     //
    //     row_group_writer.close()?;
    // }
    // // while let Some(mut column_writer) = row_group_writer.next_column()? {
    // //     let columns = row?.get_column_iter().into_columns();
    // //     column_writer.typed().write_batch(&columns.1, None, None)?;
    // //     column_writer.close()?;
    // // }
    // writer.close()?;
    Ok(())
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
