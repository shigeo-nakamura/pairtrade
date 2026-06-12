use std::fs::File;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, RawFd};

pub(crate) struct StdioRedirect {
    _stdout: FdRedirect,
    _stderr: FdRedirect,
}

impl StdioRedirect {
    pub(crate) fn to_file(log_file: &File) -> io::Result<Self> {
        io::stdout().flush()?;
        io::stderr().flush()?;

        let stdout = FdRedirect::redirect(io::stdout().as_raw_fd(), log_file.as_raw_fd())?;
        let stderr = FdRedirect::redirect(io::stderr().as_raw_fd(), log_file.as_raw_fd())?;
        Ok(Self {
            _stdout: stdout,
            _stderr: stderr,
        })
    }
}

struct FdRedirect {
    target_fd: RawFd,
    saved_fd: RawFd,
}

impl FdRedirect {
    fn redirect(target_fd: RawFd, source_fd: RawFd) -> io::Result<Self> {
        let saved_fd = dup_fd(target_fd)?;
        if let Err(err) = dup2_fd(source_fd, target_fd) {
            close_fd(saved_fd);
            return Err(err);
        }
        Ok(Self {
            target_fd,
            saved_fd,
        })
    }

    fn restore(&mut self) -> io::Result<()> {
        if self.saved_fd < 0 {
            return Ok(());
        }

        let saved_fd = self.saved_fd;
        self.saved_fd = -1;
        let restore_result = dup2_fd(saved_fd, self.target_fd);
        close_fd(saved_fd);
        restore_result
    }
}

impl Drop for FdRedirect {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

fn dup_fd(fd: RawFd) -> io::Result<RawFd> {
    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(duplicated)
    }
}

fn dup2_fd(source_fd: RawFd, target_fd: RawFd) -> io::Result<()> {
    if unsafe { libc::dup2(source_fd, target_fd) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn close_fd(fd: RawFd) {
    let _ = unsafe { libc::close(fd) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::panic;

    fn fd_target(fd: RawFd) -> std::path::PathBuf {
        std::fs::read_link(format!("/proc/self/fd/{fd}")).expect("read fd link")
    }

    #[test]
    fn fd_redirect_restores_original_fd_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().join("original.log");
        let redirected_path = dir.path().join("redirected.log");
        let original = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&original_path)
            .unwrap();
        let redirected = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&redirected_path)
            .unwrap();
        let target_fd = original.as_raw_fd();

        let before = fd_target(target_fd);
        {
            let _guard = FdRedirect::redirect(target_fd, redirected.as_raw_fd()).unwrap();
            assert_eq!(fd_target(target_fd), redirected_path);
        }

        assert_eq!(fd_target(target_fd), before);
    }

    #[test]
    fn fd_redirect_restores_original_fd_after_panic() {
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().join("original.log");
        let redirected_path = dir.path().join("redirected.log");
        let original = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&original_path)
            .unwrap();
        let redirected = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&redirected_path)
            .unwrap();
        let target_fd = original.as_raw_fd();
        let redirected_fd = redirected.as_raw_fd();
        let before = fd_target(target_fd);

        let result = panic::catch_unwind(|| {
            let _guard = FdRedirect::redirect(target_fd, redirected_fd).unwrap();
            assert_eq!(fd_target(target_fd), redirected_path);
            panic!("forced panic while redirected");
        });

        assert!(result.is_err());
        assert_eq!(fd_target(target_fd), before);
    }
}
