Embulk::JavaPlugin.register_filter(
  "scala_pass", "org.embulk.filter.ScalaPassFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
