import React, { useEffect, useState } from 'react';
import axios from 'axios';
import '../App.css';  
// Função para formatar a data no formato 'YYYY-MM-DD'
const getBrazilianDate = () => {
  const options = { timeZone: 'America/Sao_Paulo' }; 
  const date = new Date().toLocaleDateString('pt-BR', options); 
  const [day, month, year] = date.split('/'); 
  return `${year}-${month}-${day}`;
};

const initialUserInfo = {
  Nome: '',
  Telefone: '',
  'E-mail': '', 
  Tipo: '',
  'Data de Cadastro': getBrazilianDate() 
};

function AddUser(props) {
  const [userInfo, setUserInfo] = useState(initialUserInfo);

  useEffect(() => {
  }, []);

  const addNewUser = async () => {
    try {
      const response = await axios.post(
        'https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes',
        userInfo
      );
      if (response) {
        props.setUserAdded(); 
      }
    } catch (e) {
      console.log(e);
    }
  };

  return (
    <div className="user-view _add-view">
      <h1>Basic Info</h1>
      <div className="box">
        <div className="row">
          <div className="col-sm-12 col-md-6">
            <p>
              <span>Full Name:</span>
              <input
                type="text"
                className="form-control"
                placeholder="Enter Full Name"
                value={userInfo.Nome}
                onChange={(e) => setUserInfo({ ...userInfo, Nome: e.target.value })}
              />
            </p>
          </div>
          <div className="col-sm-12 col-md-6">
            <p>
              <span>Email Address:</span>
              <input
                type="text"
                className="form-control"
                placeholder="Enter Email Address"
                value={userInfo['E-mail']} // Usando 'E-mail' no lugar de 'E_mail'
                onChange={(e) => setUserInfo({ ...userInfo, 'E-mail': e.target.value })} // Usando 'E-mail' no lugar de 'E_mail'
              />
            </p>
          </div>
          <div className="col-sm-12 col-md-6">
            <p>
              <span>Phone Number:</span>
              <input
                type="text"
                className="form-control"
                placeholder="Enter Phone Number"
                value={userInfo.Telefone}
                onChange={(e) => setUserInfo({ ...userInfo, Telefone: e.target.value })}
              />
            </p>
          </div>
          <div className="col-sm-12 col-md-6">
            <p>
              <span>Account Type:</span>
              <input
                type="text"
                className="form-control"
                placeholder="Enter Account Type"
                value={userInfo.Tipo}
                onChange={(e) => setUserInfo({ ...userInfo, Tipo: e.target.value })}
              />
            </p>
          </div>
        </div>
      </div>

      <button className="btn btn-success" onClick={addNewUser}>
        Add New User
      </button>
    </div>
  );
}

export default AddUser;
