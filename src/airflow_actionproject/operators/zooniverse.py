# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------
# Copyright (c) 2021
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import os
import json

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#--------------
# local imports
# -------------

from airflow_actionproject.hooks.zooniverse import ZooniverseHook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class ZooniverseExportOperator(BaseOperator):

	template_fields = ("_output_path",)

	@apply_defaults
	def __init__(self, output_path, conn_id, generate=False, wait=True, timeout=120, **kwargs):
		super(ZooniverseExportOperator, self).__init__(**kwargs)
		self._output_path = output_path
		self._conn_id     = conn_id
		self._generate    = generate
		self._wait        = wait
		self._timeout     = timeout


	def execute(self, context):
		with ZooniverseHook(self._conn_id) as hook:
			exported = list(
				hook.export_classifications(
					self._generate,
					self._wait,
					self._timeout
				)
			)
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, 'w') as fd:
			json.dump(exported, fp=fd, indent=2)
			self.log.info(f"Written Project Classification export to {self._output_path}")
