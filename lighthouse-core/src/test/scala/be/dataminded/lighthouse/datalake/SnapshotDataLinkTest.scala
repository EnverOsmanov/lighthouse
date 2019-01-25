package be.dataminded.lighthouse.datalake

import java.time.LocalDate
import java.time.Month.DECEMBER

import be.dataminded.lighthouse.testing.SparkFunSuite
import org.scalatest.Matchers

class SnapshotDataLinkTest extends SparkFunSuite with Matchers {

  test("A snapshot of DataLink should not duplicate date") {
    val link = new FileSystemDataLink(path = "./target/output/orc")
      .snapshotOf(LocalDate.of(1982, DECEMBER, 21))
      .snapshotOf(LocalDate.of(1982, DECEMBER, 21))

    link.path() shouldBe "./target/output/orc/1982/12/21/"
  }

}
