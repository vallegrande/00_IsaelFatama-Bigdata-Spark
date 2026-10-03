package pe.edu.vallegrande.bigdata.spark.core.config;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PermissiveLocalFileSystem extends LocalFileSystem {

    public PermissiveLocalFileSystem() {
        super(new Raw());
    }

    public static boolean needed() {
        return System.getProperty("os.name").toLowerCase().contains("win")
                && System.getenv("HADOOP_HOME") == null
                && System.getProperty("hadoop.home.dir") == null;
    }

    public static class Raw extends RawLocalFileSystem {

        @Override
        public void setPermission(Path path, FsPermission permission) {
        }

        @Override
        public void setOwner(Path path, String user, String group) {
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            File file = existing(path);
            return new FileStatus(file.length(), file.isDirectory(), 1, getDefaultBlockSize(path), file.lastModified(),
                    makeQualified(path));
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            File file = existing(path);
            if (file.isFile()) {
                return new FileStatus[]{getFileStatus(path)};
            }
            String[] names = file.list();
            if (names == null) {
                return new FileStatus[0];
            }
            FileStatus[] children = new FileStatus[names.length];
            for (int index = 0; index < names.length; index++) {
                children[index] = getFileStatus(new Path(path, names[index]));
            }
            return children;
        }

        private File existing(Path path) throws FileNotFoundException {
            File file = pathToFile(path);
            if (!file.exists()) {
                throw new FileNotFoundException("No existe " + path);
            }
            return file;
        }
    }
}
