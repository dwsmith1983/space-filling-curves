/*
 * Copyright 2021 DustinSmith.Io. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
val projectName = "dustinsmith-space-filling-curves"
val projectVersion = "0.1.0"

lazy val sparkVersion = "3.1.0"
lazy val scalatestVersion = "3.2.3"

// https://github.com/djspiewak/sbt-github-packages/issues/24
// githubTokenSource := TokenSource.GitConfig("github.token")  || TokenSource.Environment("GITHUB_TOKEN")

lazy val commonSettings = Seq(
  organization := "io.dustinsmith",
  scalaVersion := "2.12.13",
  version := projectVersion
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := projectName,
    libraryDependencies ++= {
      List(
        "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
        "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
        "org.scalaz" %% "scalaz-core" % "7.3.3",

        // Logging
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
        "ch.qos.logback" % "logback-classic" % "1.2.3",
        "org.slf4j" % "log4j-over-slf4j" % "1.7.26",

        // For unit testing
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "org.scalatest" %% "scalatest-shouldmatchers" % scalatestVersion % Test,
        "org.scalatestplus" %% "scalacheck-1-15" % "3.2.3.0" % Test,
        "org.scalacheck" %% "scalacheck" % "1.15.2" % Test
      )
    }
  )

resolvers += Resolver.githubPackages("dwsmith1983", projectName)
githubOwner := "dwsmith1983"
githubRepository := projectName
