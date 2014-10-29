package io.criticality.cookbook.scala

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation
import java.io.File
import java.io.BufferedWriter
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.FileWriter
import java.io.InputStreamReader
import java.security.PrivilegedExceptionAction
import scala.io.Source
import scala.collection.JavaConversions._
import java.io.StringWriter
import scala.collection.mutable.ListBuffer

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

  def saveFile(filepath: String, remote: String): String = {
    val file = new File(filepath)
    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        val out = new BufferedOutputStream(fileSystem.create(new Path(remote)))
        for (line <- Source.fromFile(file).getLines()) {
          var b = line.getBytes();
          while (b.length > 0) {
            out.write(b)
          }
        }
        out.close()
      }
    })
    remote
  }

  def exists(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.exists(path)
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    var sucess = false
    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        sucess = fileSystem.delete(path, true)
      }
    })
    sucess
  }

  def getFile(remote: String, local: String): String = {
    val path = new Path(remote)
    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        val rd = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
        val wt = new BufferedWriter(new FileWriter(local))
        var line: String = rd.readLine
        while (line != null) {
          wt.write(line);
          line = rd.readLine
        }
        rd.close
        wt.close
      }
    })
    local
  }

  /**
   * read a file to a String
   *
   */
  def readFile(remote: String): String = {
    val path = new Path(remote)

    val wt = new StringWriter()

    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {

      def run: Unit = {
        val rd = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
        var line: String = rd.readLine
        while (line != null) {
          wt.write(line + "\n");
          line = rd.readLine
        }
        rd.close
      }

    })
    wt.flush

    val result = wt.toString
    wt.close
    result
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    var sucess = false
    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        if (!fileSystem.exists(path)) {
          sucess = fileSystem.mkdirs(path)
        }
      }
    })
    sucess
  }

  /**
   * retrieves the entire output of a map reduce operation designated by the @param remote folder
   */
  def getOutput(remote: String, local: String): String = {

    val status = List.fromArray(fileSystem.listStatus(new Path(remote)))
    System.out.println("Found " + status.size + " in " + remote);
    val wt = new BufferedWriter(new FileWriter(local))
    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        status.foreach(item => {
          if (item.getPath().toString().contains("part")) {
            val br = new BufferedReader(new InputStreamReader(fileSystem.open(item.getPath())))
            var line: String = br.readLine
            while (line != null) {
              System.out.println(line);
              wt.append(line).append("\n");
              line = br.readLine()
            }
            br.close
          }
        })
        wt.close
      }
    })
    local

  }

  /**
   * retrieves the list of part* files of a map reduce operation designated by the @param remote folder
   */
  def listFiles(remote: String): List[String] = {

    val status = List.fromArray(fileSystem.listStatus(new Path(remote)))
    val local = new ListBuffer[String]
    System.out.println("Found " + status.size + " in " + remote);
    val ugi = UserGroupInformation.createRemoteUser(getHdfsUser)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        status.foreach(item => {
          if (item.getPath().toString().contains("part")) {
            local += item.getPath().toString()
          }
        })

      }
    })
    local.toList
  }

  def getHdfsUser(): String = {
    conf.get("hadoop.job.ugi");
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    for (token <- source.getTokens()) {
      dest.addToken(token)
    }
  }
}
