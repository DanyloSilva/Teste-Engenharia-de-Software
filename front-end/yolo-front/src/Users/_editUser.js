import React, { useEffect, useState } from 'react';
import axios from 'axios';

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
  'Data de Cadastro': getBrazilianDate(),
};

function UpdateUser({ userId, setUserUpdated, handleUserUpdate, setShowEditMode  }) {
  const [userInfo, setUserInfo] = useState(initialUserInfo);
  const [updateSuccess, setUpdateSuccess] = useState(false);

  useEffect(() => {
    const fetchUserData = async () => {
      try {
        const response = await axios.get(
          `https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/cliente`,
          { params: { clientesId: userId } }
        );
        setUserInfo(response.data);
      } catch (error) {
        console.error('Erro ao buscar os dados do usuário:', error);
      }
    };

    if (userId) fetchUserData();
  }, [userId]);
  
  const onCloseDialog = () => {
    setShowEditMode(false);
  };
  

  const updateUser = async () => {

    alert("Dado Atualizado com sucesso!");
    try {
      const updates = [];
    
      if (userInfo.Nome) updates.push({ updateKey: 'Nome', updateValue: userInfo.Nome });
      if (userInfo['E-mail']) updates.push({ updateKey: 'E-mail', updateValue: userInfo['E-mail'] });
      if (userInfo.Telefone) updates.push({ updateKey: 'Telefone', updateValue: userInfo.Telefone });
      if (userInfo.Tipo) updates.push({ updateKey: 'Tipo', updateValue: userInfo.Tipo });
    
      for (const update of updates) {
        await axios.patch(
          `https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes`,
          {
            clientesId: userId,
            updateKey: update.updateKey,
            updateValue: update.updateValue,
          }
        );
      }
    
      setUpdateSuccess(true);
      setUserUpdated();  
      handleUserUpdate();
    
   
      onCloseDialog(); 
  
      setUserInfo({ ...userInfo }); 
    } catch (error) {
      console.error('Erro ao atualizar os dados do usuário:', error);
    }
    window.location.reload();
  };
  

  return (
    <div className="container py-4">
      <h1 className="mb-4">Editar</h1>

      {updateSuccess && (
        <div className="alert alert-success" role="alert">
          Dados atualizados com sucesso!
        </div>
      )}

      <div className="box shadow p-4 rounded">
        <div className="row">
          <div className="col-sm-12 col-md-6 mb-3">
            <label htmlFor="nome">Full Name:</label>
            <input
              id="nome"
              type="text"
              className="form-control"
              placeholder="Enter Full Name"
              value={userInfo.Nome}
              onChange={(e) => setUserInfo({ ...userInfo, Nome: e.target.value })}
            />
          </div>
          <div className="col-sm-12 col-md-6 mb-3">
            <label htmlFor="email">Email Address:</label>
            <input
              id="email"
              type="text"
              className="form-control"
              placeholder="Enter Email Address"
              value={userInfo['E-mail']}
              onChange={(e) => setUserInfo({ ...userInfo, 'E-mail': e.target.value })}
            />
          </div>
          <div className="col-sm-12 col-md-6 mb-3">
            <label htmlFor="telefone">Phone Number:</label>
            <input
              id="telefone"
              type="text"
              className="form-control"
              placeholder="Enter Phone Number"
              value={userInfo.Telefone}
              onChange={(e) => setUserInfo({ ...userInfo, Telefone: e.target.value })}
            />
          </div>
          <div className="col-sm-12 col-md-6 mb-3">
            <label htmlFor="tipo">Account Type:</label>
            <input
              id="tipo"
              type="text"
              className="form-control"
              placeholder="Enter Account Type"
              value={userInfo.Tipo}
              onChange={(e) => setUserInfo({ ...userInfo, Tipo: e.target.value })}
            />
          </div>
        </div>
      </div>

      <button
        className="btn btn-success mt-3"
        onClick={updateUser}
      >
        Update User
      </button>
    </div>
  );
}

export default UpdateUser;
