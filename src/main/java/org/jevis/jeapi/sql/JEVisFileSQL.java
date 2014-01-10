/**
 * Copyright (C) 2009 - 2013 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEAPI.
 *
 * JEAPI is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * JEAPI is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * JEAPI. If not, see <http://www.gnu.org/licenses/>.
 *
 * JEAPI is part of the OpenJEVis project, further project information are
 * published at <http://www.OpenJEVis.org/>.
 */
package org.jevis.jeapi.sql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.jevis.jeapi.JEVisFile;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisFileSQL implements JEVisFile {

    private JEVisSampleSQL _sample;
    private byte[] _data;

    public JEVisFileSQL(JEVisSampleSQL sample) {
        _sample = sample;
        _data=(byte[])sample.getValue();
    }

    public void setBytes(byte[] data) {
        _sample.setValue(data);
        _data=data;
    }

    public byte[] getBytes() {
        return _data;
    }

    public void setFileExtension(String extension) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getFileExtension() {
        String[] split = _sample.getFilename().split(".");
        //TODO make it more save
        return split[split.length - 1];
    }

    public void saveToFile(File file) throws IOException {
        FileOutputStream fileOuputStream =
                new FileOutputStream(file);
        fileOuputStream.write(_data);
        fileOuputStream.close();
    }

    public void loadFromFile(File file) throws IOException {
        RandomAccessFile f = new RandomAccessFile(file, "r");
        byte[] b = new byte[(int) f.length()];
        f.read(b);
        _sample.setValue(b);
        _sample.SetFilename(file.getName());

    }

    public String getFilename() {
        return _sample.getFilename();
    }

    public void setFilename(String name) {
        _sample.SetFilename(name);
    }
}
