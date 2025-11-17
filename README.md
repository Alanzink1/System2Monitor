# 📦 **System2Monitor – Conversor de Base DJSystem → DJMonitor**

**System2Monitor** é um utilitário completo desenvolvido em **Lazarus/FreePascal**, criado para **converter automaticamente** os arquivos DBF do sistema legado da DJSystem para o banco de dados moderno utilizado pelo **DJMonitor/DJPDV (Firebird .FDB)**.

Projetado especialmente para migrar lojas com segurança, reaproveitando cadastros, movimentos e histórico sem retrabalho.

---

## 🚀 **Principais Recursos**
<img width="979" height="511" alt="image" src="https://github.com/user-attachments/assets/5d7eea25-bef8-4d61-81d3-b0e149dfa065" />

O conversor executa automaticamente a importação da maioria dos módulos necessários para continuar o trabalho via DJMonitor:

### **1. Cadastros Básicos**
- Grupos  
- Marcas  
- Vendedores  
- Carteiras  
- Tributações (ICMS, CST, CSOSN, CFOP, ST…)  
- Transportadoras e veículos  
- Ramo de atividade  

### **2. Cadastros Gerais**
- Clientes  
- Classes de clientes  
- Fornecedores  
- Grades  
- Produtos (GTIN, alternativos, ICMS, estoque, NCM, CEST…)  

### **3. Documentos**
- Pré-venda
- Terminais  
- Turnos  
- Documentos de venda  
- Formas de pagamento utilizadas  
- Entradas

### **4. Financeiro**
- Faturas  
- Contas a Receber
- Contas a Pagar ( em breve )
---

## 🛠️ **Como o Conversor Funciona**

1. O programa solicita o **diretório onde estão os DBFs do DJSystem**.  
2. Cria automaticamente o arquivo **DJPDV.FDB** 4.1.1 zerado, caso não exista.  
3. Abre conexão Firebird com:
   ```
   Username: sysdba
   Password: masterkey
   Charset: UTF8
   ```
4. Executa cada módulo de importação na ordem correta, garantindo que:
   - não haja duplicidades  
   - códigos existentes sejam preservados  
   - dados inconsistentes sejam corrigidos automaticamente  
   - campos inválidos sejam sanitizados  
   - o banco final esteja pronto para uso no DJMonitor  

---

## 📁 **Arquitetura dos Módulos**

Cada importador segue o padrão:

```
ImportX(conn, trans, query, systemPath);
```

Isso garante:

- Reutilização da conexão  
- Controle transacional  
- Baixa duplicação  
- Padronização  
- Manutenção facilitada  

---

## 🧠 **Resumo Técnico dos Módulos**

### **ImportGroups / ImportBrands**
Importa grupos e marcas com ordenação por código.

### **ImportSellers**
Importa vendedores com comissão e flags padrão.

### **ImportWallets**
Importa carteiras com regras de antecipação, juros e descontos.

### **ImportTaxations**
Converte toda estrutura fiscal:
- CST  
- CSOSN  
- ICMS  
- IVA  
- ST  
- FCP  
- CFOP  
- Origem  

### **ImportClients**
Importa clientes, classes, estrutura de endereço, créditos e documentos.

### **ImportProducts**
- Gera código de barras automático  
- Valida GTIN  
- Cria códigos alternativos  
- Calcula estoque total  
- Ajusta NCM e CEST  
- Define ICMS e situação tributária  

### **Documentos e Movimentações**
- Importa pré-vendas  
- Itens de pré-venda  
- Documentos de venda  
- Itens de documentos de venda
- Formas de pagamento  
- Entradas  
- Itens de entrada  

### **ImportAccountsReceivable**
Importa faturas, parcelas e vínculos com clientes.

---

## ▶️ **Como Usar**

1. Baixe o arquivo compactado do repositório, descompacte e compile via Lazarus (ou executando o .exe), devendo conter a pasta assets com o dentro banco_zerado.fdb, no mesmo diretório:
   ```
   /assets
       /banco_zerado.fdb
   System2Monitor.exe
   ```
2. Deixe também na mesma pasta ou em um local acessível todos os DBFs do DJSystem.  
3. Execute:
   ```
   System2Monitor.exe
   ```
4. Informe o caminho:
   ```
   C:\dj\DJSystem
   ```
5. Aguarde o processo.

Após finalizar, o arquivo **DJPDV.FDB** estará pronto para uso no DJMonitor.

---

## 🧪 **Status do Projeto**

O conversor está funcional, porém ainda em desenvolvimento.   

---

## 🧑‍💻 **Tecnologias Utilizadas**

- Lazarus / FreePascal  
- Firebird 5.0+  
- TDbf  
- SQLDB / IBConnection  
- Windows

---

## ⭐ **Objetivo**

Reduzir drasticamente o tempo de implantação, aumentar a precisão dos dados migrados e evitar retrabalhos.

---
