package io.criticality.cookbook.scala.avro

import java.io.{ByteArrayInputStream, File}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.file.DataFileReader

class AvroUtilities {

  def writeAvro[A <: SpecificRecordBase](records: Seq[A], schema: Schema, path: String) {
    val writer = new DataFileWriter(new SpecificDatumWriter[A](schema))
    writer.create(schema, new File(path))
    records.foreach {
      record => writer.append(record);
    }
    writer.flush
    writer.close
  }

  def readAvro[A <: SpecificRecordBase](path: String, schema: Schema): Seq[A] = {
    val datum = new SpecificDatumReader[A](schema);
    val reader = new DataFileReader[A](new File(path), datum);
    val results = Seq.newBuilder[A]
    while (reader.hasNext) {
      results += reader.next()
    }
    results.result
  }

  def readAvro[A <: SpecificRecordBase](input: Array[Byte], schema: Schema): Seq[A] = {
    val bais = new ByteArrayInputStream(input);
    val datum = new SpecificDatumReader[A](schema);
    val reader = new DataFileReader[A](input, datum);
    val results = Seq.newBuilder[A]
    while (reader.hasNext) {
      results += reader.next()
    }
    results.result
  }

}