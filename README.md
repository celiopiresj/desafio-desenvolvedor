
# FinanceHub API

FinanceHub API é uma aplicação desenvolvida com FastAPI para gerenciar arquivos, incluindo upload, listagem e filtragem de arquivos com base em diversos parâmetros.

## Instalação

Para executar o FinanceHub API, você precisa ter o Docker e o Docker Compose instalados. Siga os passos abaixo:

1. Clone este repositório:
   ```bash
   git clone https://github.com/celiopiresj/desafio-desenvolvedor.git financehub-api
   cd financehub-api
   git checkout origin/celio-pires-junior
   ```

2. Construa e inicie os containers:
   ```bash
   docker-compose up --build
   ```

3. Acesse a API:
   - Documentação da API: [http://localhost:8000/docs](http://localhost:8000/docs)

## Endpoints

### Raiz

- **GET /**: Redireciona para a documentação da API.

### Listar Arquivos

- **GET /files/**: Lista todos os arquivos.
  - **Parâmetros**:
    - `page`: Número da página a ser exibida (default: 1).
    - `page_size`: Número de arquivos por página (default: 10).

### Listar Histórico de Arquivos

- **GET /files/history**: Lista todo o histórico de arquivos.
  - **Parâmetros**:
    - `page`: Número da página a ser exibida (default: 1).
    - `page_size`: Número de arquivos por página (default: 10).

### Upload de Arquivo

- **POST /files/upload/**: Faz o upload de um arquivo.
  - **Body**: Arquivo a ser enviado.

### Obter Arquivo por Nome

- **GET /files/filename/{filename}**: Obtém um arquivo pelo nome.
  - **Parâmetros**:
    - `exact_filename_match`: Se verdadeiro, busca por correspondência exata (default: true).
    - `include_content`: Se verdadeiro, inclui o conteúdo do arquivo na resposta (default: false).
    - `paginate`: Se verdadeiro, aplica paginação aos resultados (default: true).
    - `page`: Número da página a ser exibida (default: 1).
    - `page_size`: Número de arquivos por página (default: 10).

### Obter Arquivo por Data de Upload

- **GET /files/upload_date/{upload_date}**: Obtém arquivos pela data de upload.
  - **Parâmetros**:
    - `include_content`: Se verdadeiro, inclui o conteúdo do arquivo na resposta (default: false).
    - `paginate`: Se verdadeiro, aplica paginação aos resultados (default: true).
    - `page`: Número da página a ser exibida (default: 1).
    - `page_size`: Número de arquivos por página (default: 10).

### Obter Arquivo por Campos

- **GET /files/fields**: Obtém arquivos com base em campos filtrados.
  - **Parâmetros**: 
    - `fields`: Parâmetros de filtro para busca.

### Deletar Arquivo

- **DELETE /files/{filename}**: Deleta um arquivo pelo nome.
  - **Parâmetros**:
    - `filename`: O nome do arquivo a ser deletado.
