package com.suitesoftware.psa.catalogservice.impl;

import com.suitesoftware.psa.catalogservice.dto.Catalog;
import com.suitesoftware.psa.catalogservice.dto.Part;
import org.apache.log4j.Logger;

import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class FeedReturnStreamingOutput implements StreamingOutput {

    private Logger log = Logger.getLogger(getClass());
    File file;
    Date modifiedSince;
    public FeedReturnStreamingOutput(File file, Date modifiedSince) {
        this.file = file;
        this.modifiedSince = modifiedSince;
    }
    @Override
    public void write(OutputStream outputStream)  {
        try {
            FileInputStream input = new FileInputStream (file);
            ReadableByteChannel source = input.getChannel();
            WritableByteChannel dest = Channels.newChannel(outputStream);
            if(modifiedSince == null) { // write the whole file
                ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);
                while (source.read(buffer) != -1) {
                    // Prepare the buffer to be drained
                    buffer.flip();
                    // Make sure that the buffer was fully drained
                    while (buffer.hasRemaining()) {
                        dest.write(buffer);
                    }
                    // Make the buffer empty, ready for filling
                    buffer.clear();
                }
            } else {
                // filter the results, gotta parse, filter, write

                JAXBContext jaxbContext = JAXBContext.newInstance( Catalog.class );
                Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                Catalog catalog = (Catalog)jaxbUnmarshaller.unmarshal(file);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                catalog.setModifiedSince(sdf.format(modifiedSince));
                List<Part> partList = new LinkedList<>();
                for(Part part : catalog.getPartList().getPart()) {
                    if(!part.getLastModified().before(modifiedSince)) {
                        partList.add(part);
                    }
                }
                catalog.getPartList().setPart(partList);
                Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
                jaxbMarshaller.marshal(catalog,outputStream);
                log.info("filtered stream complete: " + modifiedSince.toString());
            }
            source.close();
            dest.close();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}
