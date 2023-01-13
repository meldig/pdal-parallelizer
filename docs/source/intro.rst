Introduction
================================================

pdal-parallelizer is a tool that allows you to process your point clouds through pipelines that will be executed on several cores of your machine.
This tool uses the `flexible open-source Python library Dask <https://www.dask.org//>`__  for the multiprocess side and allows you to use the power
of the `Point Data Abstraction Library, PDAL <https://pdal.io/en/stable/>`__ to write your pipelines.

It also protect you from any problem during the execution. Indeed, as the points clouds treatments can be really long, if something goes wrong during
the execution you don't want to restart this from the beginning. So pdal-parallelizer will serialize each pipeline to protect you from this.

pdal-parallelizer Github repository : https://github.com/meldig/pdal-parallelizer

pdal-parallelizer license
-------------------------

Copyright (c) 2022, Métropole Européenne de Lille
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

- Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived fromthis software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.