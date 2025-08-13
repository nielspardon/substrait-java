rootProject.name = "substrait"

includeBuild("build-logic")

includeBuild("build-graal")

include("bom", "core", "isthmus", "isthmus-cli", "spark", "examples:substrait-spark")
