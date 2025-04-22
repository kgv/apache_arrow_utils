use anyhow::Result;
use std::fs::File;

#[test]
fn write_ipc_via_polars() -> Result<()> {
    use polars::prelude::*;
    use polars_arrow::array::Utf8ViewArray;

    let data_type = create_enum_dtype(Utf8ViewArray::from_slice_values(["A", "B"]));
    let mut data_frame = df! {
        "EnumColumn" => [
            Some(Series::from_iter(["A", "B"]).cast(&data_type)?),
        ],
    }?;
    let file = File::create("temp.arrow")?;
    let mut writer = IpcWriter::new(file);
    writer.finish(&mut data_frame)?;
    Ok(())
}

#[test]
fn read_ipc_via_arrow() -> Result<()> {
    use arrow::ipc::reader::FileReader;

    let file = File::open("temp.arrow")?;
    let reader = FileReader::try_new(file, None)?;
    let schema = reader.schema();
    println!("schema: {schema:#?}");
    // Error: Parser error: Unable to get root as message: Unaligned { position:
    // 100, unaligned_type: "i64", error_trace: ErrorTrace([TableField {
    // field_name: "variadicBufferCounts", position: 72 }, TableField {
    // field_name: "data", position: 40 }, UnionVariant { variant:
    // "MessageHeader::DictionaryBatch", position: 16 }, TableField {
    // field_name: "header", position: 16 }]) }
    Ok(())
}
