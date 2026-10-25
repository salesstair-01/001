//! An object store in a local directory, for development and tests
//! (`-store-dir`). It is object_store's LocalFileSystem plus the one thing
//! SlateDB needs that it lacks: conditional update (`PutMode::Update`,
//! used for SlateDB's GC boundary). An update takes an exclusive lock on a
//! per-object lock file, so it is atomic across processes sharing the
//! directory, then compares the ETag and replaces the object.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Debug)]
pub struct LocalStore {
    inner: Arc<LocalFileSystem>,
    locks: PathBuf,
}

impl LocalStore {
    pub fn new(root: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(root)?;
        let locks = PathBuf::from(root).join(".locks");
        std::fs::create_dir_all(&locks)?;
        let inner = LocalFileSystem::new_with_prefix(root).map_err(std::io::Error::other)?;
        Ok(LocalStore {
            inner: Arc::new(inner),
            locks,
        })
    }

    /// Locks location's lock file until the returned file is dropped.
    async fn lock(&self, location: &Path) -> Result<File> {
        let p = self.locks.join(location.as_ref().replace('/', "%"));
        tokio::task::spawn_blocking(move || {
            let f = OpenOptions::new().create(true).truncate(false).write(true).open(&p)?;
            f.lock()?;
            Ok::<_, std::io::Error>(f)
        })
        .await
        .map_err(|e| generic(e.to_string()))?
        .map_err(|e| generic(e.to_string()))
    }
}

fn generic(msg: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "LocalStore",
        source: msg.into(),
    }
}

impl fmt::Display for LocalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let PutMode::Update(version) = &opts.mode else {
            return self.inner.put_opts(location, payload, opts).await;
        };
        let _lock = self.lock(location).await?;
        let current = match self.inner.head(location).await {
            Ok(m) => m,
            Err(object_store::Error::NotFound { path, source }) => {
                return Err(object_store::Error::Precondition { path, source });
            }
            Err(e) => return Err(e),
        };
        if current.e_tag != version.e_tag {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: format!("ETag {:?} is not {:?}", current.e_tag, version.e_tag).into(),
            });
        }
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            ..opts
        };
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
