package org.embulk.filter

import org.embulk.config.{ConfigSource, Task, TaskSource}
import org.embulk.spi._

class ScalaPassFilterPlugin extends FilterPlugin {
  trait PluginTask extends Task {
  }

  override def transaction(config: ConfigSource, inputSchema: Schema,
                           control: FilterPlugin.Control) {
    val task = config.loadConfig(classOf[PluginTask]);
    val outputSchema = inputSchema
    control.run(task.dump(), outputSchema)
  }

  override def open(taskSource: TaskSource, inputSchema: Schema, outputSchema: Schema, output: PageOutput): PageOutput = {
    new PageOutput() {
      val pageReader: PageReader = new PageReader(inputSchema)
      val pageBuilder: PageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output)

      override def finish() = {
        pageBuilder.finish()
      }

      override def close() = {
        pageBuilder.close()
      }

      override def add(page: Page) = {
        pageReader.setPage(page);
        val visitor = new ColumnVisitorImpl(pageBuilder);
        while (pageReader.nextRecord()) {
          outputSchema.visitColumns(visitor)
          pageBuilder.addRecord()
        }
      }

      class ColumnVisitorImpl(pb: PageBuilder) extends ColumnVisitor {
        val pageBuilder = pb

        override def booleanColumn(column: Column) = {
          if (pageReader.isNull(column)) {
            pageBuilder.setNull(column)
          } else {
            pageBuilder.setBoolean(column, pageReader.getBoolean(column));
          }
        }

        override def longColumn(column: Column) = {
          if (pageReader.isNull(column)) {
            pageBuilder.setNull(column)
          } else {
            pageBuilder.setLong(column, pageReader.getLong(column));
          }
        }

        override def doubleColumn(column: Column) = {
          if (pageReader.isNull(column)) {
            pageBuilder.setNull(column)
          } else {
            pageBuilder.setDouble(column, pageReader.getLong(column));
          }
        }

        override def stringColumn(column: Column) = {
          if (pageReader.isNull(column)) {
            pageBuilder.setNull(column)
          } else {
            pageBuilder.setString(column, pageReader.getString(column));
          }
        }

        override def timestampColumn(column: Column) = {
          if (pageReader.isNull(column)) {
            pageBuilder.setNull(column)
          } else {
            pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
          }
        }
      }
    }
  }
}
