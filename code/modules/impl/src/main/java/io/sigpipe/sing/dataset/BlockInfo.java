/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package io.sigpipe.sing.dataset;

import io.sigpipe.sing.dataset.Block;
import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.Serializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A simple utility for reading and reporting information about a Galileo
 * {@link Block}.
 *
 * @author malensek
 */
public class BlockInfo {

    public static void main(String[] args)
    throws FileNotFoundException, IOException, SerializationException {

        File f = new File(args[0]);
        FileInputStream fIn = new FileInputStream(f);
        byte[] blockData = new byte[(int) f.length()];
        fIn.read(blockData);
        fIn.close();

        Block block = Serializer.deserialize(Block.class, blockData);
        System.out.println("Metadata Block:");
        System.out.println("---------------");
        System.out.println(block.getMetadata());

        System.out.println("Block Content:");
        System.out.println("--------------");
        try {
            /* If the Block contains Metadata as its content, go ahead and print
             * that out too.  Otherwise, we'll just print info about the binary
             * information. */
            Metadata metaContent = Serializer.deserialize(Metadata.class,
                    block.getData());
            System.out.println();
            System.out.println(metaContent);
        } catch (Exception e) {
            System.out.println("(binary block)");
            System.out.println("Byte length: " + block.getData().length);
        }
    }
}
