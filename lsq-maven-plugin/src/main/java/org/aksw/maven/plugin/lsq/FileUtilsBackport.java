package org.aksw.maven.plugin.lsq;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.aksw.commons.lambda.throwing.ThrowingConsumer;

/** FIXME Delete this class as it has been moved to aksw-commons */
public class FileUtilsBackport {
    public static void moveAtomicIfSupported(Consumer<String> warnCallback, Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            if (warnCallback != null) {
                warnCallback.accept(String.format("Atomic move from %s to %s failed, falling back to copy", source, target));
            }
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** Actions if the target already exists */
    public static enum OverwriteMode {
        /** Raise an error */
        ERROR,

        /** Overwrite the target */
        OVERWRITE,

        /** Skip the write */
        SKIP
    }

    public static void safeCreate(Path target, OverwriteMode overwriteAction, ThrowingConsumer<OutputStream> writer) throws Exception {
        safeCreate(target, null, overwriteAction, writer);
    }

    public static void safeCreate(Path target, Function<OutputStream, OutputStream> encoder, OverwriteMode overwriteAction, ThrowingConsumer<OutputStream> writer) throws Exception {
        Objects.requireNonNull(overwriteAction);

        String fileName = target.getFileName().toString();
        String tmpFileName = "." + fileName + ".tmp"; // + new Random().nextInt();
        Path tmpFile = target.resolveSibling(tmpFileName);

        Boolean fileExists = OverwriteMode.SKIP.equals(overwriteAction) || OverwriteMode.ERROR.equals(overwriteAction)
                ? Files.exists(target)
                : null;

        // Check whether the target already exists before we start writing the tmpFile
        if (Boolean.TRUE.equals(fileExists) && OverwriteMode.ERROR.equals(overwriteAction)) {
            throw new FileAlreadyExistsException(target.toAbsolutePath().toString());
        }

        if (!(Boolean.TRUE.equals(fileExists) && OverwriteMode.SKIP.equals(overwriteAction))) {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            boolean allowOverwrite = OverwriteMode.OVERWRITE.equals(overwriteAction);
            // What to do if the tmp file already exists?
            try (OutputStream raw = Files.newOutputStream(tmpFile, allowOverwrite ? StandardOpenOption.CREATE : StandardOpenOption.CREATE_NEW);
                 OutputStream out = encoder != null ? encoder.apply(raw) : raw) {
                writer.accept(out);
                out.flush();
            }
            moveAtomicIfSupported(null, tmpFile, target);
        }
    }

    /** Delete a path if it is an empty directory */
    public void deleteDirectoryIfEmpty(Path path) throws IOException {
        boolean isDirectory = Files.isDirectory(path);
        if (isDirectory) {
            boolean isEmpty = Files.list(path).count() == 0;
            if (isEmpty) {
                Files.delete(path);
            }
        }
    }

}
