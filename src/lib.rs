use anyhow::Result;
use std::fs::File;

#[test]
fn write_ipc_via_polars() -> Result<()> {
    use polars::prelude::*;
    use polars_arrow::array::Utf8ViewArray;

    unsafe { std::env::set_var("POLARS_FMT_MAX_ROWS", "256") };
    unsafe { std::env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256") };
    unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "256") };

    let data_type = create_enum_dtype(Utf8ViewArray::from_slice_values(["S", "D"]));
    // let mut data_frame = df! {
    //     "Bounds" => [
    //         df! {
    //             "Index" => Series::from_iter([Some(1), Some(2), None]),
    //             "Bound" => Series::from_iter(["A", "B", "A"]).cast(&data_type)?,
    //         }?.into_struct(PlSmallStr::EMPTY).into_series(),
    //     ],
    // }?;
    // let mut data_frame = df! {
    //     "Bounds" => [
    //         df! {
    //             "Bound" => Series::from_iter(["D", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S"]).cast(&data_type)?,
    //             "Index" => Series::from_iter([Option::<u8>::None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]),
    //         }?.into_struct(PlSmallStr::EMPTY).into_series().sort(SortOptions::default().with_nulls_last(true))?,
    //     ],
    // }?;
    let mut data_frame = df! {
        "Bounds" => [
            df! {
                "Index" => Series::from_iter([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                "Bound" => Series::from_iter(["D", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S"]).cast(&data_type)?,
            }?.into_struct(PlSmallStr::EMPTY).into_series(),
            df! {
                "Index" => Series::new_null(PlSmallStr::EMPTY, 15),
                "Bound" => Series::from_iter(["D", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S"]).cast(&data_type)?,
            }?.into_struct(PlSmallStr::EMPTY).into_series(),
        ],
    }?;
    // data_frame = data_frame
    //     .lazy()
    //     .select([as_struct(vec![col("EnumColumn")])])
    //     .collect()?;
    println!("data_frame: {data_frame}");
    // let file = File::create("temp.arrow")?;
    // let mut writer = IpcWriter::new(file);
    // writer.finish(&mut data_frame)?;
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
