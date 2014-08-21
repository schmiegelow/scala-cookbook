package io.criticality.cookbook.scala
import java.io.File
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import scala.io.Source
import java.io.BufferedWriter
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.FileWriter
import java.io.InputStreamReader

/**
 * A set of utility classes for HDFS file system access
 *
 */
object HdfsUtilities {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = new BufferedOutputStream(fileSystem.create(new Path(file.getName)))
    for (line <- Source.fromFile(file).getLines()) {
      var b = line.getBytes();
      while (b.length > 0) {
        out.write(b)
      }
    }
    out.close()
  }

  def exists(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.exists(path)
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(remote: String, local: String): String = {
    val path = new Path(remote)
    val rd = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    val wt = new BufferedWriter(new FileWriter(local))
    var line: String = rd.readLine
    while (line != null) {
      wt.write(line);
      line = rd.readLine
    }
    rd.close
    wt.close
    local
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}