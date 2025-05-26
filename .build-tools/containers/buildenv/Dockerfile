ARG ARCH=
FROM registry.gitlab.huaweirc.ch/zrc-von-neumann-lab/runtime-system-innovations/hicr/buildenv-${ARCH}:latest

ENV USER=hicr
ENV HOME=/home/$USER

USER $USER 

RUN sudo apt purge -y libopenblas-dev
RUN sudo apt purge -y liblapack-dev
RUN sudo apt purge -y liblapacke-dev
RUN sudo apt autoremove -y
RUN rm -rf /usr/share/doc/libopenblas  /usr/lib/libopenblas* /usr/lib/pkconfig/openblas.pc 
RUN git clone -b v0.3.28 --single-branch --depth 1 https://github.com/OpenMathLib/OpenBLAS.git $HOME/OpenBLAS && \
  cd $HOME/OpenBLAS && \
  sed -i " s/# USE_THREAD = 0/USE_THREAD=0/g " Makefile.rule && \
  sed -i " s/# USE_LOCKING = 1/USE_LOCKING=1/g " Makefile.rule && \
  make && \
  sudo make PREFIX=/usr/local install && \
  sudo rm -rf ${HOME}/OpenBLAS

WORKDIR $HOME