package com.phylosoft.iot

import com.phylosoft.iot.AppConfig.Mode
import com.phylosoft.iot.utils.AbstractParams

case class Params(environment: String = null,
                  mode: Mode.Mode = Mode.REALTIME,
                  inputPath: String = null,
                  outputPath: String = null)
  extends AbstractParams[Params]

