echo "###################################################################"
echo "INSTALLING xcube-cci-${XCUBE_CCI_VERSION} using mode $XCUBE_CCI_INSTALL_MODE"
echo "###################################################################"


if [[ $XCUBE_CCI_INSTALL_MODE == "branch" ]]; then
  git clone https://github.com/dcs4cop/xcube-cci
  cd xcube-cci || exit
  git checkout "${XCUBE_CCI_VERSION}"

  # Had been used to deal with invalid environment definitions
  # sed -i 's/xcube/#xcube/g' environment.yml

  mamba env update -n cate-env
  source activate cate-env
  pip install .

  cd .. || exit

  rm -rf xcube-cci
elif [[ $XCUBE_CCI_INSTALL_MODE == "github" ]]; then
  rm -rf v"${XCUBE_CCI_VERSION}".tar.gz
  wget https://github.com/dcs4cop/xcube-cci/archive/v"${XCUBE_CCI_VERSION}".tar.gz
  tar xvzf v"${XCUBE_CCI_VERSION}".tar.gz

  cd xcube-cci-"${XCUBE_CCI_VERSION}" || exit

  ls -al

  # Had been used to deal with invalid environment definitions
  # sed -i 's/xcube/#xcube/g' environment.yml

  mamba env update -n cate-env

  source activate cate-env

  python setup.py install

  cd ..
  rm v"${XCUBE_CCI_VERSION}".tar.gz
else
  mamba update -y -n cate-env -c conda-forge xcube-cci="${XCUBE_CCI_VERSION}"
fi

# used to ensure all necessary dependencies are included
mamba install -c conda-forge -y affine click rasterio pydap strict-rfc3339 urllib3 lxml aiohttp nest-asyncio
