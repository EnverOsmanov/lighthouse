package be.dataminded.lighthouse.datalake

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, Dataset}

class SnapshotDataLink(dataLink: PathBasedDataLink, val date: LazyConfig[LocalDate], val pathSuffix: Option[String])
    extends PathBasedDataLink {

  override def snapshotOf(date: LazyConfig[LocalDate], pathSuffix: Option[String]): SnapshotDataLink =
    new SnapshotDataLink(dataLink, date, pathSuffix)

  val path: LazyConfig[String] =
    s"${dataLink.path().trim().stripSuffix("/")}/${date().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))}/${pathSuffix
      .getOrElse("")}"

  override def doRead(path: String): DataFrame = dataLink.doRead(path)

  override def doWrite[T](data: Dataset[T], path: String): Unit = dataLink.doWrite(data, path)
}
