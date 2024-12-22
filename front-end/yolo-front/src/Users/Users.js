import React, { useState, useEffect } from 'react';
import { DataTable } from 'primereact/datatable';
import { Column } from 'primereact/column';
import { Dropdown } from 'primereact/dropdown';
import axios from 'axios';
import { Dialog } from 'primereact/dialog';
import AddUser from './_addUser';
import EditUser from './_editUser';
import { ConfirmDialog } from 'primereact/confirmdialog';
import { confirmDialog } from 'primereact/confirmdialog';
import '../App.css';

function Users() {
    const [users, setUsersList] = useState([]);
    const [filteredUsers, setFilteredUsers] = useState([]);
    const [userTypes, setUserTypes] = useState([]);
    const [selectedType, setSelectedType] = useState(null);
    const [showAddMode, setShowAddMode] = useState(false);
    const [showEditMode, setShowEditMode] = useState(false);
    const [selectedUserId, setSelectedUserId] = useState(null);

    const [updateDialogVisible, setUpdateDialogVisible] = useState(false);
    const [updateMessage, setUpdateMessage] = useState('');
    const [showSuccessMessage, setShowSuccessMessage] = useState(false); 


    useEffect(() => {
        getAllUsers();
    }, []);

    const getAllUsers = async () => {
        try {
            const response = await axios.get('https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes');
            if (response && response.data && response.data.clientes) {
                const usersData = response.data.clientes;
                setUsersList(usersData);
                setFilteredUsers(usersData);
    
                const uniqueTypes = [...new Set(usersData.map((user) => user.Tipo))];
                setUserTypes(uniqueTypes);
            }
        } catch (e) {
            console.log("Erro ao fazer a requisição:", e);
        }
    };
    
    const setUserUpdated = () => {
        getAllUsers(); 
        setShowEditMode(false);
      };
      
    const filterByType = (type) => {
        setSelectedType(type);
        if (type) {
            const filtered = users.filter((user) => user.Tipo === type);
            setFilteredUsers(filtered);
        } else {
            setFilteredUsers(users);
        }
    };

    const actionsTemplate = (rowData) => {
        return (
            <>
                <button className='btn btn-primary' onClick={() => {
                    setSelectedUserId(rowData.clientesId); // Certifique-se de usar `clientesId`
                    setShowEditMode(true);
                }}>
                    <i className='pi pi-file-edit'></i>
                </button>
                <button className='btn btn-danger' onClick={() => deleteUserConfirm(rowData)}>
                    <i className='pi pi-trash'></i>
                </button>
            </>
        );
    };

    const deleteUserConfirm = (rowData) => {
        confirmDialog({
            message: 'Are you sure you want to delete this user?',
            header: 'Confirmation',
            icon: 'pi pi-trash',
            accept: () => deleteUser(rowData),
        });
    };

    const deleteUser = async (rowData) => {
        try {
            const response = await axios.delete('https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes', {
                data: { clientesId: rowData.clientesId },
            });
            if (response.status === 200) {
                getAllUsers(); // Recarregar a lista de usuários
            } else {
                console.error("Erro ao excluir: Status inesperado", response.status);
            }
        } catch (e) {
            console.error("Erro ao excluir usuário:", e.response?.data || e.message || e);
        }
    };

    const updateBase = async () => {
        try {
            const response = await axios.get('https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/load_external_data');
            setUpdateMessage(response.data.message || 'Base atualizada com sucesso!');
            setUpdateDialogVisible(true);
        } catch (e) {
            setUpdateMessage('Erro ao atualizar a base: ' + (e.message || e));
            setUpdateDialogVisible(true);
        }
    };

    const handleUserUpdate = () => {
        setShowSuccessMessage(true); 
        setTimeout(() => {
            setShowSuccessMessage(false);
        }, 3000);
        getAllUsers(); 
    };

    return (
        <div className='users-page'>
            <div className='container'>
                <h1>Yolo - Coliving</h1>
                <h3>Clientes</h3>

                <div className='users-list'>
                    <div className='addNewUser'>
                        <button className='btn-custom' style={{ marginRight: '16px' }} onClick={() => setShowAddMode(true)}>
                            <i className='pi pi-plus'></i> Adicionar
                        </button>
                        <button className='btn-custom' onClick={updateBase}>
                            Atualizar a Base
                        </button>
                        <Dropdown
                            value={selectedType}
                            options={userTypes.map((type) => ({ label: type, value: type }))}
                            onChange={(e) => filterByType(e.value)}
                            placeholder="Filtrar por Tipo"
                            style={{ marginLeft: '16px' }}
                        />
                    </div>
                    <DataTable value={filteredUsers}>
                        <Column field="Nome" header="Nome"></Column>
                        <Column field="E-mail" header="E-mail"></Column>
                        <Column field="Telefone" header="Telefone"></Column>
                        <Column field="Tipo" header="Tipo"></Column>
                        <Column field="Data de Cadastro" header="Data de Cadastro"></Column>
                        <Column header="Ações" body={actionsTemplate}></Column>
                    </DataTable>

                    {/* Exibir a mensagem de sucesso após a atualização */}
                    {showSuccessMessage && (
                        <div className="alert alert-success">
                            Usuário atualizado com sucesso!
                        </div>
                    )}
                </div>
            </div>

            <Dialog header="Add New User"
                visible={showAddMode}
                style={{ width: '50vw' }}
                onHide={() => setShowAddMode(false)}>
                <AddUser setUserAdded={() => {
                    setShowAddMode(false);
                    getAllUsers();
                }} />
            </Dialog>
            <Dialog header="Editar Usuário"
  visible={showEditMode}
  style={{ width: '50vw' }}
  onHide={() => setShowEditMode(false)}>
  {selectedUserId && (
    <EditUser
      userId={selectedUserId}
      setUserUpdated={setUserUpdated}
      handleUserUpdate={handleUserUpdate} // Passa a função handleUserUpdate
      setShowEditMode={setShowEditMode} // Passa setShowEditMode como prop
    />
  )}
</Dialog>


            <Dialog header="Atualização da Base"
                visible={updateDialogVisible}
                style={{ width: '50vw' }}
                onHide={() => setUpdateDialogVisible(false)}>
                <p>{updateMessage}</p>
            </Dialog>

            <ConfirmDialog />
        </div>
    );
}

export default Users;
