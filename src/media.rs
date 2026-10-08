//! Media type classification by file extension.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Image,
    Video,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Image => "image",
            Kind::Video => "video",
        }
    }
}

const IMAGE_EXT: &[&str] = &[
    "jpg", "jpeg", "jpe", "png", "gif", "webp", "bmp", "tif", "tiff", "avif", "heic", "heif", "jxl", "ico", "svg",
];
const VIDEO_EXT: &[&str] = &[
    "mp4", "m4v", "mov", "mkv", "webm", "avi", "wmv", "flv", "mpg", "mpeg", "3gp", "ts", "m2ts", "mts", "ogv",
];

/// Lower-cased extension of a file name (without dot), or "".
pub fn extension(name: &str) -> String {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => ext.to_ascii_lowercase(),
        _ => String::new(),
    }
}

pub fn classify(ext: &str) -> Option<Kind> {
    if IMAGE_EXT.contains(&ext) {
        Some(Kind::Image)
    } else if VIDEO_EXT.contains(&ext) {
        Some(Kind::Video)
    } else {
        None
    }
}

/// Best-effort MIME type: the server's value if usable, else guessed from the extension.
pub fn mime_for(ext: &str, server: Option<&str>, kind: Kind) -> String {
    if let Some(m) = server {
        let m = m.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
        let ok = match kind {
            Kind::Image => m.starts_with("image/"),
            Kind::Video => m.starts_with("video/"),
        };
        if ok {
            return m;
        }
    }
    mime_guess::from_ext(ext).first().map(|m| m.essence_str().to_string()).unwrap_or_else(|| match kind {
        Kind::Image => "image/octet-stream".into(),
        Kind::Video => "video/octet-stream".into(),
    })
}

/// Split "/a/b/c.jpg" into ("/a/b", "c.jpg"); top-level files have dir "/".
pub fn split_path(path: &str) -> (String, String) {
    match path.rsplit_once('/') {
        Some(("", name)) => ("/".to_string(), name.to_string()),
        Some((dir, name)) => (dir.to_string(), name.to_string()),
        None => ("/".to_string(), path.to_string()),
    }
}

pub fn fingerprint(path: &str, size: i64, mtime: i64) -> String {
    format!("{path}|{size}|{mtime}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_and_kind() {
        assert_eq!(extension("a.JPG"), "jpg");
        assert_eq!(extension(".hidden"), "");
        assert_eq!(classify("mp4"), Some(Kind::Video));
        assert_eq!(classify("txt"), None);
        assert_eq!(split_path("/a/b/c.jpg"), ("/a/b".into(), "c.jpg".into()));
        assert_eq!(split_path("/c.jpg"), ("/".into(), "c.jpg".into()));
    }
}
