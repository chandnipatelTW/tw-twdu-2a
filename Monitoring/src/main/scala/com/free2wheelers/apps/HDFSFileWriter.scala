package com.free2wheelers.apps

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

object HDFSFileWriter {

  def write(path: String, data: String) {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val output = fs.create(new Path(path))
      val writer = new PrintWriter(output)
      writer.write(data.toString)
      writer.close()
  }
}