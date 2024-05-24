package org.aksw.maven.plugin.lsq;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class RDFDataMgrExBackport {
    public static OutputStream encode(OutputStream out, List<String> codecs, CompressorStreamFactory csf)
            throws CompressorException {
        OutputStream result = out;
        for (String encoding : codecs) {
            result = csf.createCompressorOutputStream(encoding, result);
        }
        return result;
    }

    public static Function<OutputStream, OutputStream> encoder(String ... codecs) {
        List<String> list = Arrays.asList(codecs);
        return encoder(list);
    }

    public static Function<OutputStream, OutputStream> encoder(List<String> codecs) {
        CompressorStreamFactory csf = CompressorStreamFactory.getSingleton();
        return encoder(csf, codecs);
    }

    public static Function<OutputStream, OutputStream> encoder(CompressorStreamFactory csf, List<String> codecs) {
        return out -> {
            try {
                return encode(out, codecs, csf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
