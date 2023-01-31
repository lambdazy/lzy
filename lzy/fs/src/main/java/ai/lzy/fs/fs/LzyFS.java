package ai.lzy.fs.fs;

import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.posix.util.Platform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseException;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.channels.ClosedChannelException;
import java.nio.file.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("FieldMayBeFinal")
public class LzyFS extends FuseStubFS {
    private static final Logger LOG = LogManager.getLogger(LzyFS.class);

    private static final int BLOCK_SIZE = 4096;
    private static long userId;
    private static long groupId;
    private static long startTime;

    static { // startup black magic
        try {
            startTime = System.currentTimeMillis();
            userId = Long.parseLong(lineCmd("id -u"));
            groupId = Long.parseLong(lineCmd("id -g"));
        } catch (IOException | InterruptedException e) {
            LOG.warn("Unable to get group and user id on startup");
        }
    }

    private Map<Path, Set<String>> children = Collections.synchronizedMap(new HashMap<>());
    private Set<String> roots = Collections.synchronizedSet(new HashSet<>());
    private Map<Path, LzyScript> executables = Collections.synchronizedMap(new HashMap<>());
    private Map<Path, LzyFileSlot> slots = Collections.synchronizedMap(new HashMap<>());
    private Map<Long, FileContents> openFiles = Collections.synchronizedMap(new HashMap<>());
    private Map<Path, Set<Long>> filesOpen = Collections.synchronizedMap(new HashMap<>());
    private AtomicLong lastFh = new AtomicLong(1000);

    public LzyFS() {
    }

    public LzyFS(Set<String> roots) {
        children.put(Path.of("/"), new HashSet<>(roots));
        this.roots.addAll(roots);
        for (String root : roots) {
            children.put(Paths.get("/", root), new HashSet<>());
        }
    }

    public static int executeUnsafe(UnsafeIOOperation op) {
        return executeUnsafeInt(() -> {
            op.execute();
            return 0;
        });
    }

    public static int executeUnsafeInt(UnsafeIntIOOperation op) {
        try {
            try {
                return op.execute();
            } catch (IOException ioe) {
                LOG.info("IOE", ioe);
                throw ioe;
            }
        } catch (FileNotFoundException fnfe) {
            if (fnfe.getMessage().contains("Is a directory")) {  // standard message for such cases
                return -ErrorCodes.EISDIR();
            }
            return -ErrorCodes.ENOENT();
        } catch (NoSuchFileException nsfe) {
            return -ErrorCodes.ENOENT();
        } catch (FileAlreadyExistsException faee) {
            return -ErrorCodes.EEXIST();
        } catch (DirectoryNotEmptyException dnee) {
            return -ErrorCodes.ENOTEMPTY();
        } catch (AccessDeniedException ade) {
            return -ErrorCodes.EACCES();
        } catch (ClosedChannelException ce) {
            return -ErrorCodes.EBADF();
        } catch (IOException e) {
            LOG.warn("Unexpected exception during I/O operation", e);
            return -ErrorCodes.EIO();
        }
    }

    public static String lineCmd(String cmd) throws IOException, InterruptedException {
        final Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
        try (LineNumberReader rd = new LineNumberReader(new InputStreamReader(p.getInputStream()))) {
            return rd.readLine();
        }
    }

    @Override
    public void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts) {
        super.mount(mountPoint, blocking, debug, fuseOpts);
    }

    @Override
    public void umount() {
        if (Platform.IS_WINDOWS) {
            super.umount();
            return;
        }

        if (!mounted.get()) {
            return;
        }

        final var mountPath = mountPoint.toAbsolutePath().toString();
        Process p;
        try {
            p = new ProcessBuilder("fusermount", "-u", "-z", mountPath).start();
        } catch (IOException e) {
            p = null;
            try {
                new ProcessBuilder("umount", mountPath).start().waitFor();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new FuseException("Unable to umount FS", e);
            } catch (IOException ioe) {
                ioe.addSuppressed(e);
                throw new FuseException("Unable to umount FS", ioe);
            }
        }

        if (p != null) {
            try {
                var rc = p.waitFor();
                if (rc != 0) {
                    try {
                        p.getErrorStream().transferTo(System.err);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FuseException("Unable to umount FS", e);
            }
        }

        mounted.set(false);
    }

    public boolean addScript(LzyScript script, Path path) {
        if (executables.put(path, script) == null) {
            return addPath(path);
        }
        return false;
    }

    public void addSlot(LzyFileSlot slot) {
        addPath(slot.location());
        slots.put(slot.location(), slot);
    }

    public void removeSlot(String name) {
        final Path path = Paths.get(name);
        if (slots.remove(path) == null) {
            return;
        }

        Path parent = path.getParent();
        name = path.getFileName().toString();
        if (!children.getOrDefault(parent, new HashSet<>()).remove(name)) {
            return;
        }

        while (parent != null) {
            final Set<String> children = this.children.computeIfAbsent(parent, p -> new HashSet<>());
            if (!children.remove(name)) {
                break;
            }
            if (!children.isEmpty()) {
                break;
            }
            this.children.remove(parent);
            if (parent.getFileName() == null) { // at root
                break;
            }
            name = parent.getFileName().toString();
            parent = parent.getParent();
        }
    }

    private boolean addPath(Path path) {
        String name = path.getFileName().toString();
        Path parent = path.getParent();
        if (children.getOrDefault(parent, Set.of()).contains(name)) {
            return false;
        }

        while (parent != null) {
            final Set<String> children = this.children.computeIfAbsent(parent, p -> new HashSet<>());
            if (children.contains(name)) {
                break;
            }
            children.add(name);
            if (parent.getFileName() == null) {  // at root
                break;
            }
            name = parent.getFileName().toString();
            parent = parent.getParent();
        }
        return true;
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        return -ErrorCodes.EACCES();
    }

    @Override
    public int mkdir(String pathStr, @mode_t long mode) {
        final Path path = Paths.get(pathStr);
        if (!children.containsKey(path.getParent())) {
            return -ErrorCodes.ENOENT();
        }
        return addPath(path) ? 0 : -ErrorCodes.EALREADY();
    }

    @Override
    public int open(String pathStr, FuseFileInfo fi) {
        final Path path = Path.of(pathStr);
        final long fh = lastFh.addAndGet(1);
        if (executables.containsKey(path)) {
            final FileContents open = new TextContents(path, executables.get(path).scriptText());
            openFiles.put(fh, open);
            filesOpen.computeIfAbsent(path, p -> new HashSet<>()).add(fh);
            fi.fh.set(fh);
            return 0;
        } else if (slots.containsKey(path)) {
            return executeUnsafe(() -> {
                final FileContents open = slots.get(path).open(fi);
                openFiles.put(fh, open);
                filesOpen.computeIfAbsent(path, p -> new HashSet<>()).add(fh);
                fi.fh.set(fh);
            });
        }

        return -ErrorCodes.ENOENT();
    }

    @Override
    public int release(String pathStr, FuseFileInfo fi) {
        final FileContents contents = openFiles.remove(fi.fh.longValue());
        try {
            if (contents == null) {
                return -ErrorCodes.EBADFD();
            }
            contents.close();
            final Path path = Paths.get(pathStr);
            final Set<Long> fhs = filesOpen.get(path);
            fhs.remove(fi.fh.longValue());
            if (fhs.isEmpty()) {
                filesOpen.remove(path);
            }
            return 0;
        } catch (IOException ioe) {
            return -ErrorCodes.EBADF();
        }
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        final FileContents contents;
        contents = openFiles.get(fi.fh.longValue());
        if (contents == null) {
            return -ErrorCodes.EBADFD();
        }
        return executeUnsafeInt(() -> contents.read(buf, offset, size));
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        final FileContents contents;
        contents = openFiles.get(fi.fh.longValue());
        if (contents == null) {
            return -ErrorCodes.EBADFD();
        }
        return executeUnsafeInt(() -> contents.write(buf, offset, size));
    }

    @SuppressWarnings("OctalInteger")
    @Override
    public int getattr(String pathStr, FileStat stat) {
        final Path path = Path.of(pathStr).toAbsolutePath();
        long time = startTime;
        if (children.containsKey(path) || path.equals(Path.of("/"))) { // directory
            stat.st_mode.set(0750 | FileStat.S_IFDIR);
            final long size = children.getOrDefault(path, Set.of()).stream().mapToLong(String::length).sum() + 64;
            stat.st_size.set(size);
        } else if (executables.containsKey(path)) { // declared operation
            final LzyScript executable = executables.get(path);
            stat.st_mode.set(0750 | FileStat.S_IFREG);
            stat.st_size.set(executable.scriptText().length());
        } else if (slots.containsKey(path)) {
            final LzyFileSlot slot = slots.get(path);
            time = -1;
            {
                final long mtime = slot.mtime();
                stat.st_mtim.tv_sec.set(TimeUnit.MILLISECONDS.toSeconds(mtime));
                stat.st_mtim.tv_nsec.set(TimeUnit.MILLISECONDS.toNanos(mtime));
            }
            {
                final long atime = slot.atime();
                stat.st_atim.tv_sec.set(TimeUnit.MILLISECONDS.toSeconds(atime));
                stat.st_atim.tv_nsec.set(TimeUnit.MILLISECONDS.toNanos(atime));
            }
            {
                final long ctime = slot.ctime();
                stat.st_ctim.tv_sec.set(TimeUnit.MILLISECONDS.toSeconds(ctime));
                stat.st_ctim.tv_nsec.set(TimeUnit.MILLISECONDS.toNanos(ctime));
            }
            stat.st_mode.set(0640 | slot.mtype());
            stat.st_size.set(4096); //set page size & disable caches
        } else {
            return -ErrorCodes.ENOENT();
        }

        stat.st_uid.set(userId);
        stat.st_gid.set(groupId);

        stat.st_blksize.set(BLOCK_SIZE);
        stat.st_blocks.set((long) Math.ceil(stat.st_size.longValue() / (double) BLOCK_SIZE));
        if (time > 0) {
            stat.st_mtim.tv_sec.set(TimeUnit.MILLISECONDS.toSeconds(time));
            stat.st_mtim.tv_nsec.set(TimeUnit.MILLISECONDS.toNanos(time));
            stat.st_atim.tv_sec.set(TimeUnit.MILLISECONDS.toSeconds(time));
            stat.st_atim.tv_nsec.set(TimeUnit.MILLISECONDS.toNanos(time));
        }
        return 0;
    }

    @Override
    public int readdir(String pathStr, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        final Path path = Paths.get(pathStr);
        final Set<String> children = this.children.getOrDefault(path, Set.of());
        if (children == null) {
            return this.executables.containsKey(path) || this.slots.containsKey(path) ? -ErrorCodes.ENOTDIR() :
                -ErrorCodes.ENOENT();
        }
        children.stream().sorted().forEach(child -> {
            final FileStat lstat = new FileStat(buf.getRuntime());
            if (getattr(path.resolve(child).toString(), lstat) == 0) {
                filter.apply(buf, child, lstat, 0);
            }
        });
        return 0;
    }

    @Override
    public int chmod(String path, long mode) {
        return -ErrorCodes.EACCES();
    }

    @Override
    public int chown(String path, long uid, long gid) {
        return -ErrorCodes.EACCES();
    }

    @Override
    public int utimens(String path, Timespec[] timespec) {
        return -ErrorCodes.EACCES();
    }

    // TODO: implement for default version of executable
    //@Override
    //public int readlink(String path, Pointer buf, long size) {
    //}
    @Override
    public int statfs(String path, Statvfs stbuf) {
        stbuf.f_bsize.set(BLOCK_SIZE);
        stbuf.f_frsize.set(1024 * BLOCK_SIZE);
        stbuf.f_blocks.set(Integer.MAX_VALUE);
        stbuf.f_bfree.set(Integer.MAX_VALUE);
        stbuf.f_bavail.set(Integer.MAX_VALUE);

        stbuf.f_flag.set(Statvfs.ST_NODEV | Statvfs.ST_WRITE | Statvfs.ST_NOATIME | Statvfs.ST_NODIRATIME);
        return 0;
    }

    @Override
    public int rename(String path, String newName) {
        return -ErrorCodes.EACCES();
    }

    @Override
    public int rmdir(String path) {
        return -ErrorCodes.EACCES();
    }

    @Override
    public int truncate(String pathStr, long offset) {
        final Path path = Paths.get(pathStr);
        if (slots.containsKey(path)) {
            return 0;
        }
        return -ErrorCodes.EACCES();
    }

    @Override
    public int unlink(String pathStr) {
        final Path path = Paths.get(pathStr);
        if (filesOpen.containsKey(path)) {
            return -ErrorCodes.EBUSY();
        }
        if (children.containsKey(path)) {
            if (children.get(path).isEmpty()) {
                children.remove(path);
                final Path parent = path.getParent();
                if (parent != null) {
                    children.get(parent).remove(path.getFileName().toString());
                } else {
                    roots.remove(path.getFileName().toString());
                }
            }
        } else if (executables.containsKey(path)) {
            return -ErrorCodes.EACCES();
        } else if (slots.containsKey(path)) {
            final LzyFileSlot slot = slots.remove(path);
            return executeUnsafe(slot::remove);
        }
        return -ErrorCodes.ENOENT();
    }

    public interface UnsafeIOOperation {

        void execute() throws IOException;
    }

    public interface UnsafeIntIOOperation {

        int execute() throws IOException;
    }
}
