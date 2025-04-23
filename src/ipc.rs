pub(crate) mod arrow {
    use anyhow::Result;
    use arrow::ipc::reader::FileReader;
    use std::{fs::File, path::Path};

    pub(crate) fn read(path: impl AsRef<Path>) -> Result<()> {
        let file = File::open(path)?;
        let projection = None; // read all columns
        let reader = FileReader::try_new_buffered(file, projection)?;
        let schema = reader.schema();
        println!("schema: {schema:#?}");
        let schema_metadata = schema.metadata();
        println!("schema_metadata: {schema_metadata:#?}");
        let custom_metadata = reader.custom_metadata();
        println!("custom_metadata: {custom_metadata:#?}");
        for batch in reader {
            let batch = batch.unwrap();
            println!("batch: {batch:#?}");
        }
        Ok(())
    }
}

pub(crate) mod polars {
    use crate::Metadata;
    use anyhow::Result;
    use polars::prelude::*;
    use std::{fs::File, path::Path, sync::Arc};

    pub(crate) fn read(path: impl AsRef<Path>) -> Result<(Metadata, DataFrame)> {
        let file = File::open(path)?;
        let mut reader = IpcReader::new(file);
        let meta = reader
            .custom_metadata()?
            .unwrap_or_default()
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let data = reader.finish()?;
        Ok((meta, data))
    }

    pub(crate) fn read_metadata(path: impl AsRef<Path>) -> Result<Metadata> {
        let file = File::open(path)?;
        let mut reader = IpcReader::new(file);
        Ok(reader
            .custom_metadata()?
            .unwrap_or_default()
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect())
    }

    pub(crate) fn write(
        path: impl AsRef<Path>,
        meta: Metadata,
        data: &mut DataFrame,
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = IpcWriter::new(file);
        let custom_metadata = Arc::new(
            meta.into_iter()
                .map(|(key, value)| (PlSmallStr::from_string(key), PlSmallStr::from_string(value)))
                .collect(),
        );
        writer.set_custom_schema_metadata(custom_metadata);
        writer.finish(data)?;
        Ok(())
    }
}
