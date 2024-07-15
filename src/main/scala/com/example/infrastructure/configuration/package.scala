package com.example.infrastructure

import com.typesafe.config.{Config, ConfigFactory}

package object configuration {
  lazy val AppConfig: Config = ConfigFactory.load()
}
