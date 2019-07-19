package com.ms.silverking.os.linux.proc;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import com.ms.silverking.io.StreamParser;

/**
 * Reads a single line file from /proc (e.g. /proc/<pid>/stat) into a
 * corresponding Java class that is effectively a C-style struct.
 */
public class StructReader<T> {
    private final Class<T> _class;
    
    public StructReader(Class<T> _class) {
        this._class = _class;
    }

    public T read(File file) throws IOException {
        return read(StreamParser.parseLine(file));
    }

    public T read(String def) {
        return read(def.split("\\s+"));
    }

    public T read(String[] tokens) {
        Field[] fields;
        int     i;
        T       newObject;
        
        fields = _class.getFields();
        i = 0;
        try {
            newObject = (T)_class.newInstance();
            while (i < tokens.length && i < fields.length) {
                String  fieldType;
                
                fieldType = fields[i].getType().getName();
                try {
                    if (fieldType.equals("char")) {
                        fields[i].setChar(newObject, tokens[i].charAt(0));
                    } else if (fieldType.equals("long")) {
                        fields[i].setLong(newObject, Long.parseLong(tokens[i]));
                    } else if (fieldType.equals("int")) {
                        fields[i].setInt(newObject, Integer.parseInt(tokens[i]));
                    } else if (fieldType.equals("java.lang.String")) {
                        fields[i].set(newObject, tokens[i]);
                    } else {
                        throw new RuntimeException("Unsupported field type: "+ fieldType);
                    }
                    //System.out.println(fieldType +"\t"+ fields[i].get(newObject));
                } catch (NumberFormatException nfe) {
                    //System.out.println("format bad: "+ fieldType);
                }
                i++;
            }
            return newObject;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
     
    // unit testing
    public static void main(String[] args) {
        try {
            StructReader<ProcessStat>   reader;
            
            reader = new StructReader<ProcessStat>(ProcessStat.class);
            reader.read(new File("/proc/1/stat"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
