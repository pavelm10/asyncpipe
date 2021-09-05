DIR="venv"
if [ -d ${DIR} ]
then
  echo "Removing ${DIR}..."
  rm -rf ${DIR}
fi

echo "Creating virtual environment..."
python3.9 -m venv ${DIR}
source ${DIR}/bin/activate
echo "Installing packages from reqs.txt"
pip install -r reqs.txt
