program System2Monitor;
{$codepage UTF8}

uses
  IBConnection, SQLDB, DB, dbf, SysUtils, Windows, Classes, Contnrs, Math;

type
  TCodDesc = class
    Cod: Integer;
    Desc: String;
  end;

  TSeller = class
    Cod, Flag: Integer;
    Commission: Double;
    Name: String;
  end;

  TWallet = class
    Cod, DiasAnt, Carencia, TipoJuros: Integer;
    Multa, JurosDia: Double;
    Tipo, TipoDoc: Char;
    Desc, Obs, Mensagem: String;
  end;

  TTaxation = class
    Cod, Modbc, ModbCST, Csosn, BaseICMS, MotDesICMS, Origem: Integer;
    BaseFcp, AliqFcp, AliqICMS, PMVast, BaseSt, AliqSt, BaseUfDest, AliqFcpUfDest, AliqUfDest, BcfCpuFDest, Diferimento: Double;
    Desc, Cfop, Cst: String;
  end;

function CompareGrupo(Item1, Item2: Pointer): Integer;
begin
  Result := TCodDesc(Item1).Cod - TCodDesc(Item2).Cod;
end;

procedure CreateEmptyDatabase;
begin
  if not FileExists('./DJPDV.FDB') then
    Windows.CopyFile('./assets/banco_zerado.fdb', './DJPDV.FDB', False);
end;

procedure ImportGroups(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfFile: TDbf;
  List: TObjectList;
  item: TCodDesc;
  codGrupo: Integer;
  descGrupo, codGrupoStr: String;
  i: Integer;
begin
  dbfFile := TDbf.Create(nil);
  List := TObjectList.Create(True);
  dbfFile.FilePathFull := ExtractFilePath(systemPath + '/familia.dbf');
  dbfFile.TableName := ExtractFileName(systemPath + '/familia.dbf');
  dbfFile.Open;

  dbfFile.First;
  while not dbfFile.EOF do
  begin
    descGrupo := Trim(dbfFile.FieldByName('DESCRICAO').AsString);
    codGrupoStr := Trim(dbfFile.FieldByName('GRUPO').AsString);
    codGrupo := StrToIntDef(codGrupoStr, 0);

    item := TCodDesc.Create;
    item.Cod := codGrupo;
    item.Desc := descGrupo;

    List.Add(item);

    dbfFile.Next;
  end;

  List.Sort(@CompareGrupo);

  trans.StartTransaction;

  for i := 0 to List.Count - 1 do
  begin
    item := TCodDesc(List[i]);

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM GRUPO WHERE CODGRUPO = :COD';
    query.ParamByName('COD').AsInteger := item.Cod;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO GRUPO (CODGRUPO, DESCRICAO) VALUES (:COD, :DESC)';
      query.ParamByName('COD').AsInteger := item.Cod;
      query.ParamByName('DESC').AsString := Copy(item.Desc, 1, 50);
      query.ExecSQL;
    end;

    query.Close;
  end;

  trans.Commit;

  List.Free;
  if dbfFile.Active then dbfFile.Close;
  FreeAndNil(dbfFile);
  writeln('Importação de grupos concluída.');
end;

procedure ImportBrands(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfFile: TDbf;
  List: TObjectList;
  item: TCodDesc;
  codBrand: Integer;
  descBrand, codBrandStr: String;
  i: Integer;
begin
  dbfFile := TDbf.Create(nil);
  List := TObjectList.Create(True);
  dbfFile.FilePathFull := ExtractFilePath(systemPath + '/marca.dbf');
  dbfFile.TableName := ExtractFileName(systemPath + '/marca.dbf');
  dbfFile.Open;

  dbfFile.First;
  while not dbfFile.EOF do
  begin
    descBrand := Trim(dbfFile.FieldByName('DESCRICAO').AsString);
    codBrandStr := Trim(dbfFile.FieldByName('CODMAR').AsString);
    codBrand := StrToIntDef(codBrandStr, 0);

    item := TCodDesc.Create;
    item.Cod := codBrand;
    item.Desc := descBrand;

    List.Add(item);

    dbfFile.Next;
  end;

  List.Sort(@CompareGrupo);

  trans.StartTransaction;

  for i := 0 to List.Count - 1 do
  begin
    item := TCodDesc(List[i]);

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM MARCA WHERE CODMARCA = :COD';
    query.ParamByName('COD').AsInteger := item.Cod;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO MARCA (CODMARCA, DESCRICAO) VALUES (:COD, :DESC)';
      query.ParamByName('COD').AsInteger := item.Cod;
      query.ParamByName('DESC').AsString := Copy(item.Desc, 1, 50);
      query.ExecSQL;
    end;

    query.Close;
  end;

  trans.Commit;

  List.Free;
  if dbfFile.Active then dbfFile.Close;
  FreeAndNil(dbfFile);
  writeln('Importação de marcas concluída.');
end;

procedure ImportSellers(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfFile: TDbf;
  List: TObjectList;
  item: TSeller;
  codSeller: Integer;
  commission: Double;
  nameSeller, codSellerStr: String;
  i: Integer;
begin
  dbfFile := TDbf.Create(nil);
  List := TObjectList.Create(True);
  dbfFile.FilePathFull := ExtractFilePath(systemPath + '/vendedor.dbf');
  dbfFile.TableName := ExtractFileName(systemPath + '/vendedor.dbf');
  dbfFile.Open;

  dbfFile.First;
  while not dbfFile.EOF do
  begin
    nameSeller := Trim(dbfFile.FieldByName('NOME').AsString);
    codSellerStr := Trim(dbfFile.FieldByName('CODVEN').AsString);
    commission := dbfFile.FieldByName('PESO_COMIS').AsFloat;
    codSeller := StrToIntDef(codSellerStr, 0);

    item := TSeller.Create;
    item.Cod := codSeller;
    item.Name := nameSeller;
    item.Flag := 0;
    item.Commission := commission;

    List.Add(item);

    dbfFile.Next;
  end;

  List.Sort(@CompareGrupo);

  trans.StartTransaction;

  for i := 0 to List.Count - 1 do
  begin
    item := TSeller(List[i]);

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM VENDEDOR WHERE CODVENDEDOR = :COD';
    query.ParamByName('COD').AsInteger := item.Cod;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO VENDEDOR (CODVENDEDOR, DESCRICAO, FLAG, COMISSAO) VALUES (:COD, :DESC, 0, :COMISSAO)';
      query.ParamByName('COD').AsInteger := item.Cod;
      query.ParamByName('DESC').AsString := Copy(item.Name, 1, 50);
      query.ParamByName('COMISSAO').AsFloat := (item.Commission * 100);
      query.ExecSQL;
    end;

    query.Close;
  end;

  trans.Commit;

  List.Free;
  if dbfFile.Active then dbfFile.Close;
  FreeAndNil(dbfFile);
  writeln('Importação de vendedores concluída.');
end;

procedure ImportWallets(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfFile: TDbf;
  List: TObjectList;
  item: TWallet;
  codWallet, diasAnt, carencia, tipoJuros: Integer;
  multa, jurosDia: Double;
  tipo, tipoDoc: Char;
  desc, codWalletStr, obs, mensagem: String;
  i: Integer;
begin
  dbfFile := TDbf.Create(nil);
  List := TObjectList.Create(True);
  dbfFile.FilePathFull := ExtractFilePath(systemPath + '/carteira.dbf');
  dbfFile.TableName := ExtractFileName(systemPath + '/carteira.dbf');
  dbfFile.Open;

  dbfFile.First;
  while not dbfFile.EOF do
  begin
    desc := Trim(dbfFile.FieldByName('DESCRICAO').AsString);
    codWalletStr := Trim(dbfFile.FieldByName('CODCAR').AsString);

    diasAnt := dbfFile.FieldByName('DIAS_DESC').AsInteger;
    carencia := dbfFile.FieldByName('CARENCIA').AsInteger;
    multa := dbfFile.FieldByName('MULTA').AsFloat;
    jurosDia := dbfFile.FieldByName('JUROS_DIA').AsFloat;

    if (multa < 0) or (multa > 30) then
      multa := 0;

    if (jurosDia < 0) or (jurosDia > 10) then
      jurosDia := 0;

    obs := Trim(dbfFile.FieldByName('OBS1').AsString);
    mensagem := Trim(dbfFile.FieldByName('OBS2').AsString);

    tipoJuros := StrToIntDef(Trim(dbfFile.FieldByName('TIPO_JUROS').AsString), 0);
    if not (tipoJuros in [0,1,2]) then
      tipoJuros := 0;

    codWallet := StrToIntDef(codWalletStr, -1);
    if codWallet <= 0 then
    begin
      writeln('Carteira ignorada (CODCAR inválido): ', codWalletStr);
      dbfFile.Next;
      Continue;
    end;

    item := TWallet.Create;
    item.Cod := codWallet;
    item.DiasAnt := diasAnt;
    item.Carencia := carencia;
    item.TipoJuros := tipoJuros;
    item.Tipo := 'R';
    item.TipoDoc := 'R';
    item.Desc := desc;
    item.Obs := obs;
    item.Mensagem := mensagem;

    item.Multa := multa;
    item.JurosDia := jurosDia;

    List.Add(item);
    dbfFile.Next;
  end;

  List.Sort(@CompareGrupo);

  trans.StartTransaction;

  for i := 0 to List.Count - 1 do
  begin
    item := TWallet(List[i]);

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM CARTEIRA WHERE CODCARTEIRA = :COD';
    query.ParamByName('COD').AsInteger := item.Cod;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
      'INSERT INTO CARTEIRA(' +
      'CODCARTEIRA, DESCRICAO, OBS, CODEXTERNO,' +
      'DIAS_ANTECIPACAO, PORC_DESC_ANTECIPACAO, DIAS_CARENCIA,' +
      'PORC_MULTA, PORC_MORA_DIARIA, PORC_PAGTO_MINIMO,' +
      'TIPO, REL_CARNE_MONITOR, REL_CARNE_PDV, IDBANCO,' +
      'TIPO_DUPLICATA, PARAMS_BOLETO, REMETENTE,' +
      'ASSUNTO, ENVIAR_COPIA, MENSAGEM,' +
      'TIPO_JUROS, APLICARDESC_RENEGOCIADAS)' +
      'VALUES (' +
      ':COD, :DESC, :OBS, 0,' +
      ':DIANTECIPACAO, 0, :DICARENCIA,' +
      ':PORCMULTA, :PORCMORADIA, 0,' +
      ':TIPO, ''N'', ''N'', 0,' +
      ':TIPODUP, '''', '''','''',''N'', :MENSAGEM,' +
      ':TIPOJUROS, ''N'')';

      query.ParamByName('COD').AsInteger := item.Cod;
      query.ParamByName('DESC').AsString := Copy(item.Desc, 1, 20);
      query.ParamByName('OBS').AsString := Copy(item.Obs, 1, 50);
      query.ParamByName('DIANTECIPACAO').AsInteger := item.DiasAnt;
      query.ParamByName('DICARENCIA').AsInteger := item.Carencia;

      query.ParamByName('PORCMULTA').AsFloat := item.Multa;
      query.ParamByName('PORCMORADIA').AsFloat := item.JurosDia;

      query.ParamByName('TIPO').AsString := 'R';
      query.ParamByName('TIPODUP').AsString := 'R';

      if Trim(item.Mensagem) = '' then
        query.ParamByName('MENSAGEM').AsString := '.'
      else
        query.ParamByName('MENSAGEM').AsString := Copy(item.Mensagem, 1, 80);

      query.ParamByName('TIPOJUROS').AsInteger := item.TipoJuros;

      query.ExecSQL;
    end;

    query.Close;
  end;

  trans.Commit;

  List.Free;
  if dbfFile.Active then dbfFile.Close;
  FreeAndNil(dbfFile);
  writeln('Importação de carteiras concluída.');
end;

procedure ImportTaxations(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfFile: TDbf;
  List: TObjectList;
  item: TTaxation;
  codTaxation, modbc, modbCST, csosn, baseICMS, motDesICMS, i: Integer;
  baseFcp, aliqFcp, aliqICMS, pmVast, baseSt, aliqSt, baseUfDest,
  aliqFcpUfDest, aliqUfDest, bcfCpuFDest, diferimento: Double;
  desc, cfop, cst, codTaxationStr: String;
begin
  dbfFile := TDbf.Create(nil);
  List := TObjectList.Create(True);
  dbfFile.FilePathFull := ExtractFilePath(systemPath + '/cfiscal.dbf');
  dbfFile.TableName := ExtractFileName(systemPath + '/cfiscal.dbf');
  dbfFile.Open;

  dbfFile.First;
  while not dbfFile.EOF do
  begin
    desc := Trim(dbfFile.FieldByName('DESCRICAO').AsString);
    cfop := Trim(dbfFile.FieldByName('CFOP').AsString);

    cst := Trim(dbfFile.FieldByName('COD_CONTAB').AsString);

    codTaxationStr := Trim(dbfFile.FieldByName('CODCFI').AsString);
    codTaxation := StrToIntDef(codTaxationStr, -1);

    modbc := StrToIntDef(Trim(dbfFile.FieldByName('MODBC').AsString), 0);
    modbCST := StrToIntDef(Trim(dbfFile.FieldByName('MODBCST').AsString), 0);
    csosn := StrToIntDef(Trim(dbfFile.FieldByName('CSOSN').AsString), 0);

    baseICMS := Round(dbfFile.FieldByName('FATOR_BASE').AsFloat * 100);

    motDesICMS := StrToIntDef(Trim(dbfFile.FieldByName('DIFER_ICMS').AsString), 0);

    baseFcp := dbfFile.FieldByName('BASE_FCP').AsFloat;
    aliqFcp := dbfFile.FieldByName('ALIQ_FCP').AsFloat;

    aliqICMS := dbfFile.FieldByName('ICMS').AsFloat;
    pmVast := dbfFile.FieldByName('IVA').AsFloat;
    baseSt := dbfFile.FieldByName('FATORST').AsFloat;
    aliqSt := dbfFile.FieldByName('ICMSST').AsFloat;

    baseUfDest := dbfFile.FieldByName('BCICMDEST').AsFloat;
    aliqFcpUfDest := dbfFile.FieldByName('FCPDEST').AsFloat;
    aliqUfDest := dbfFile.FieldByName('ICMSDEST').AsFloat;
    bcfCpuFDest := dbfFile.FieldByName('BASE_FCP').AsFloat;

    diferimento := dbfFile.FieldByName('DIFER_ICMS').AsFloat;

    item := TTaxation.Create;
    item.Cod := codTaxation;

    if Length(cst) >= 1 then
      item.Origem := StrToIntDef(Copy(cst, 1, 1), 0)
    else
      item.Origem := 0;

    if Length(cst) >= 2 then
      item.Cst := Copy(cst, Length(cst)-1, 2)
    else
      item.Cst := '00';

    item.Modbc := modbc;
    item.ModbCST := modbCST;
    item.Csosn := csosn;
    item.BaseICMS := baseICMS;
    item.MotDesICMS := motDesICMS;
    item.BaseFcp := baseFcp;
    item.AliqFcp := aliqFcp;
    item.AliqICMS := aliqICMS;
    item.PMVast := pmVast;
    item.BaseSt := baseSt;
    item.AliqSt := aliqSt;
    item.BaseUfDest := baseUfDest;
    item.AliqFcpUfDest := aliqFcpUfDest;
    item.AliqUfDest := aliqUfDest;
    item.BcfCpuFDest := bcfCpuFDest;
    item.Diferimento := diferimento;
    item.Desc := desc;
    item.Cfop := cfop;

    List.Add(item);
    dbfFile.Next;
  end;

  List.Sort(@CompareGrupo);
  trans.StartTransaction;

  for i := 0 to List.Count - 1 do
  begin
    item := TTaxation(List[i]);
    if item.Cod <= 0 then Continue;

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM ICMS WHERE ID_ICMS = :COD';
    query.ParamByName('COD').AsInteger := item.Cod;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
      'INSERT INTO ICMS(' +
      'ID_ICMS, DESCRICAO, CST, ORIGEM, MODBC, BASEICMS, ALIQICMS, MODBCST,' +
      'PMVAST, BASEST, ALIQST, MOTDESICMS, CSOSN,' +
      'BASEUFDEST, ALIQUFDEST, ALIQFCPUFDEST,' +
      'CFOP, BASEFCP, ALIQFCP, BCFCPUFDEST, DIFERIMENTO)' +
      'VALUES (' +
      ':COD, :DESC, :CST, :ORIGEM, :MODBC, :BASEICMS, :ALIQICMS, :MODBCST,' +
      ':PMVAST, :BASEST, :ALIQST, :MOTDESICMS, :CSOSN,' +
      ':BASEUFDEST, :ALIQUFDEST, :ALIQFCPUFDEST,' +
      ':CFOP, :BASEFCP, :ALIQFCP, :BCFCPUFDEST, :DIFERIMENTO)';

      query.ParamByName('COD').AsInteger := item.Cod;
      query.ParamByName('DESC').AsString := item.Desc;
      query.ParamByName('CST').AsString := item.Cst;
      query.ParamByName('ORIGEM').AsInteger := item.Origem;
      query.ParamByName('MODBC').AsInteger := item.Modbc;
      query.ParamByName('BASEICMS').AsFloat := item.BaseICMS;
      query.ParamByName('ALIQICMS').AsFloat := item.AliqICMS;
      query.ParamByName('MODBCST').AsInteger := item.ModbCST;
      query.ParamByName('PMVAST').AsFloat := item.PMVast;
      query.ParamByName('BASEST').AsFloat := item.BaseSt;
      query.ParamByName('ALIQST').AsFloat := item.AliqSt;
      query.ParamByName('MOTDESICMS').AsInteger := item.MotDesICMS;
      query.ParamByName('CSOSN').AsInteger := item.Csosn;
      query.ParamByName('BASEUFDEST').AsFloat := item.BaseUfDest;
      query.ParamByName('ALIQUFDEST').AsFloat := item.AliqUfDest;
      query.ParamByName('ALIQFCPUFDEST').AsFloat := item.AliqFcpUfDest;
      query.ParamByName('CFOP').AsString := item.Cfop;
      query.ParamByName('BASEFCP').AsFloat := item.BaseFcp;
      query.ParamByName('ALIQFCP').AsFloat := item.AliqFcp;
      query.ParamByName('BCFCPUFDEST').AsFloat := item.BcfCpuFDest;
      query.ParamByName('DIFERIMENTO').AsFloat := item.Diferimento;

      query.ExecSQL;
    end;

    query.Close;
  end;

  trans.Commit;

  writeln('Importação de tributações concluída.');

  List.Free;
  if dbfFile.Active then dbfFile.Close;
  FreeAndNil(dbfFile);
end;

procedure ImportCarriers(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfTrans: TDbf;
  dbfVeic: TDbf;
  codTraStr, nome, endereco, cidade, complemento, cep, tel,
  cnpjCpf, ie, f_j, email : String;
  codTra, i: Integer;
begin

  // TRANSPORTADORAS
  dbfTrans := TDbf.Create(nil);
  dbfTrans.FilePathFull := ExtractFilePath(systemPath + '/transpor.dbf');
  dbfTrans.TableName := ExtractFileName(systemPath + '/transpor.dbf');
  dbfTrans.Open;

  dbfTrans.First;
  while not dbfTrans.EOF do
  begin
    codTraStr := Trim(dbfTrans.FieldByName('CODTRA').AsString);
    codTra := StrToIntDef(codTraStr, -1);
    if codTra <= 0 then
    begin
      dbfTrans.Next;
      Continue;
    end;

    nome := Trim(dbfTrans.FieldByName('NOME').AsString);
    endereco := Trim(dbfTrans.FieldByName('ENDERECO').AsString);
    cidade := Trim(dbfTrans.FieldByName('CIDADE').AsString);
    complemento := Trim(dbfTrans.FieldByName('COMPLEMENT').AsString);
    cep := StringReplace(Trim(dbfTrans.FieldByName('CEP').AsString), '-', '', [rfReplaceAll]);
    tel := Trim(dbfTrans.FieldByName('TEL').AsString);
    cnpjCpf := Trim(dbfTrans.FieldByName('CGC_CPF').AsString);
    ie := Trim(dbfTrans.FieldByName('INSC_EST').AsString);

    f_j := 'F';
    email := '';

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM TRANSPORTADORA WHERE ID_TRANSPORTADORA = :COD';
    query.ParamByName('COD').AsInteger := codTra;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO TRANSPORTADORA(' +
        'ID_TRANSPORTADORA, F_J, CNPJ_CPF, XNOME, LOGRADOURO, NUMERO,' +
        'COMPLEMENTO, BAIRRO, CMUN, CEP, FONE, IE, EMAIL, CONTRIBUINTEICMS)' +
        'VALUES (' +
        ':COD, :FJ, :CNPJ, :NOME, :LOGR, '''', :COMPL, '''', :CMUN, :CEP,' +
        ':FONE, :IE, :EMAIL, 0)';

      query.ParamByName('COD').AsInteger := codTra;
      query.ParamByName('FJ').AsString := f_j;
      query.ParamByName('CNPJ').AsString := cnpjCpf;
      query.ParamByName('NOME').AsString := nome;
      query.ParamByName('LOGR').AsString := Copy(endereco, 1, 50);
      query.ParamByName('COMPL').AsString := Copy(complemento, 1, 20);
      query.ParamByName('CMUN').AsString := Copy(cidade, 1, 40);
      query.ParamByName('CEP').AsString := Copy(cep, 1, 9);
      query.ParamByName('FONE').AsString := Copy(tel, 1, 14);
      query.ParamByName('IE').AsString := ie;
      query.ParamByName('EMAIL').AsString := email;

      query.ExecSQL;
    end;

    dbfTrans.Next;
  end;

  // VEÍCULOS
  dbfVeic := TDbf.Create(nil);
  dbfVeic.FilePathFull := ExtractFilePath(systemPath + '/veiculo.dbf');
  dbfVeic.TableName := ExtractFileName(systemPath + '/veiculo.dbf');
  dbfVeic.Open;

  dbfVeic.First;
  while not dbfVeic.EOF do
  begin
    codTraStr := Trim(dbfVeic.FieldByName('CODVEIC').AsString);
    codTra := StrToIntDef(codTraStr, -1);
    if codTra <= 0 then
    begin
      dbfVeic.Next;
      Continue;
    end;

    nome := Trim(dbfVeic.FieldByName('DESCRICAO').AsString);
    cep := Trim(dbfVeic.FieldByName('PLACA').AsString);
    complemento := Trim(dbfVeic.FieldByName('OBS').AsString);

    query.Close;
    query.SQL.Text :=
      'INSERT INTO TRANSP_VEICULOS(' +
      'ID_TRANSPORTADORA, CODVEICULO, PLACA, UF, RNTC, RENAVAM, ANO,' +
      'MARCA, MODELO, COR, MOTORISTA, OBS)' +
      'VALUES (' +
      ':IDT, :CODV, :PLACA, :UF, '''', '''', 0, '''', :MODELO, '', '', :OBS)';

    query.ParamByName('IDT').AsInteger := codTra;
    query.ParamByName('CODV').AsInteger := codTra;
    query.ParamByName('PLACA').AsString := Copy(cep, 1, 10);
    query.ParamByName('UF').AsString := '';
    query.ParamByName('MODELO').AsString := Copy(nome, 1, 20);
    query.ParamByName('OBS').AsString := Copy(complemento, 1, 30);

    query.ExecSQL;

    dbfVeic.Next;
  end;

  trans.Commit;

  if dbfTrans.Active then dbfTrans.Close;
  if dbfVeic.Active then dbfVeic.Close;
  FreeAndNil(dbfTrans);
  FreeAndNil(dbfVeic);

  writeln('Importação de transportadoras e veículos concluída.');
end;

procedure ImportClients(conn: TIBConnection; trans: TSQLTransaction; query: TSQLQuery; systemPath: String);
var
  dbfClients, dbfClasses: TDbf;
  codClientStr, codClassStr, f_j, nome, classDesc, classType, apelido,
  fone, celularFax, endereco, bairro, cidade, uf, cep, obsEntrega,
  rgInscEst, cpfCnpj, obsPopup: String;
  codClient, codClass, codClasseClient, nivelCredito, contribuinteICMS: Integer;
  limiteCredito, debito: Double;
begin
  dbfClasses := TDbf.Create(nil);
  dbfClasses.FilePathFull := ExtractFilePath(systemPath + '/classe.dbf');
  dbfClasses.TableName := ExtractFileName(systemPath + '/classe.dbf');
  dbfClasses.Open;

  dbfClasses.First;
  while not dbfClasses.EOF do
  begin
    codClassStr := Trim(dbfClasses.FieldByName('CODCLA').AsString);
    codClass := StrToIntDef(codClassStr, -1);

    if codClass > 0 then
    begin
      classDesc := Trim(dbfClasses.FieldByName('DESCRICAO').AsString);
      classType := Trim(dbfClasses.FieldByName('P_N').AsString);

      query.Close;
      query.SQL.Text := 'SELECT 1 FROM CLASSE WHERE CODCLASSE = :COD';
      query.ParamByName('COD').AsInteger := codClass;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text := 'INSERT INTO CLASSE(CODCLASSE, DESCRICAO, TIPO) ' +
                          'VALUES (:COD, :DESC, :TIPO)';

        query.ParamByName('COD').AsInteger := codClass;
        query.ParamByName('DESC').AsString := classDesc;
        query.ParamByName('TIPO').AsString := classType;
        query.ExecSQL;
      end;
    end;

    dbfClasses.Next;
  end;

  dbfClients := TDbf.Create(nil);
  dbfClients.FilePathFull := ExtractFilePath(systemPath + '/cliente.dbf');
  dbfClients.TableName := ExtractFileName(systemPath + '/cliente.dbf');
  dbfClients.Open;

  dbfClients.First;

  while not dbfClients.EOF do
  begin
    codClientStr := Trim(dbfClients.FieldByName('CODCLI').AsString);
    codClient := StrToIntDef(codClientStr, -1);
    if codClient <= 0 then
    begin
      dbfClients.Next;
      Continue;
    end;

    f_j := Trim(dbfClients.FieldByName('F_J').AsString);
    nome := Trim(dbfClients.FieldByName('NOME').AsString);
    apelido := Trim(dbfClients.FieldByName('FANTASIA').AsString);
    fone := Trim(dbfClients.FieldByName('TEL_RES').AsString);
    celularFax := Trim(dbfClients.FieldByName('TEL_COB').AsString);
    endereco := Trim(dbfClients.FieldByName('ENDERE_RES').AsString);
    bairro := Trim(dbfClients.FieldByName('BAIRRO_RES').AsString);
    cidade := Trim(dbfClients.FieldByName('CIDADE_RES').AsString);
    uf := Trim(dbfClients.FieldByName('UF_RES').AsString);
    cep := Trim(dbfClients.FieldByName('CEP_RES').AsString);
    obsEntrega := Trim(dbfClients.FieldByName('LOCAL_ENTR').AsString);
    rgInscEst := Trim(dbfClients.FieldByName('RG_INSCEST').AsString);
    cpfCnpj := Trim(dbfClients.FieldByName('CIC_CGC').AsString);
    obsPopup := Trim(dbfClients.FieldByName('AVISA').AsString);

    codClasseClient := StrToIntDef(Trim(dbfClients.FieldByName('CODCLA').AsString), 0);

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM CLASSE WHERE CODCLASSE = :COD';
    query.ParamByName('COD').AsInteger := codClasseClient;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO CLASSE (CODCLASSE, DESCRICAO, TIPO) ' +
        'VALUES (:COD, ''GERAL'', ''P'')';
      query.ParamByName('COD').AsInteger := codClasseClient;
      query.ExecSQL;
    end;

    nivelCredito := dbfClients.FieldByName('NIVEL_CRED').AsInteger;

    if UpperCase(f_j) = 'F' then
      contribuinteICMS := 9
    else
      contribuinteICMS := 1;

    limiteCredito := dbfClients.FieldByName('VAL_LIMITE').AsFloat;
    debito := dbfClients.FieldByName('CREDITO').AsFloat;

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM CLIENTE WHERE CODCLIENTE = :COD';
    query.ParamByName('COD').AsInteger := codClient;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO CLIENTE(' +
        'CODCLIENTE, F_J, NOME, APELIDO_FANTASIA, FONE, CELULAR_FAX,' +
        'ENDERECO, BAIRRO, CIDADE, UF, CEP, OBS_L_ENTREGA, RG_INSCEST,' +
        'CPF_CNPJ, OBS_POPUP, CODCLASSE, NIVEL_CREDITO, FLAG,' +
        'CONTRIBUINTEICMS, LIMITE_CREDITO, DEBITO, CADASTRADO_EM)' +
        'VALUES (' +
        ':COD, :FJ, :NOME, :APELIDO, :FONE, :CELULAR, :ENDEREC, :BAIRRO,' +
        ':CIDADE, :UF, :CEP, :OBSENTREGA, :RGINCEST, :CPFPJ, :OBSPOPUP,' +
        ':CODCLASSE, :NIVELCREDITO, 0, :CONTRIBUINTEICMS,' +
        ':LIMITECREDITO, :DEBITO, :CADEM)';

      query.ParamByName('COD').AsInteger := codClient;
      query.ParamByName('FJ').AsString := f_j;
      query.ParamByName('NOME').AsString := nome;
      query.ParamByName('APELIDO').AsString := apelido;
      query.ParamByName('FONE').AsString := fone;
      query.ParamByName('CELULAR').AsString := celularFax;
      query.ParamByName('ENDEREC').AsString := endereco;
      query.ParamByName('BAIRRO').AsString := bairro;
      query.ParamByName('CIDADE').AsString := cidade;
      query.ParamByName('UF').AsString := uf;
      query.ParamByName('CEP').AsString := cep;
      query.ParamByName('OBSENTREGA').AsString := obsEntrega;
      query.ParamByName('RGINCEST').AsString := rgInscEst;
      query.ParamByName('CPFPJ').AsString := cpfCnpj;
      query.ParamByName('OBSPOPUP').AsString := obsPopup;
      query.ParamByName('CODCLASSE').AsInteger := codClasseClient;
      query.ParamByName('NIVELCREDITO').AsInteger := nivelCredito;
      query.ParamByName('CONTRIBUINTEICMS').AsInteger := contribuinteICMS;
      query.ParamByName('LIMITECREDITO').AsFloat := limiteCredito;
      query.ParamByName('DEBITO').AsFloat := debito;
      query.ParamByName('CADEM').AsDateTime := Now;

      query.ExecSQL;
    end;

    dbfClients.Next;
  end;

  trans.Commit;
  writeln('Importação de clientes concluída.');

  if dbfClients.Active then dbfClients.Close;
  if dbfClasses.Active then dbfClasses.Close;
  FreeAndNil(dbfClasses);
  FreeAndNil(dbfClients);
end;

procedure ImportActivities(conn: TIBConnection; trans: TSQLTransaction; query: TSQLQuery; systemPath: String);
var
  dbfActivities: TDbf;
  codActivityStr, activityDesc: String;
  codActivity: Integer;
begin
  dbfActivities := TDbf.Create(nil);
  dbfActivities.FilePathFull := ExtractFilePath(systemPath + '/atividad.dbf');
  dbfActivities.TableName := ExtractFileName(systemPath + '/atividad.dbf');
  dbfActivities.Open;

  dbfActivities.First;
  while not dbfActivities.EOF do
  begin
    codActivityStr := Trim(dbfActivities.FieldByName('CODATI').AsString);
    codActivity := StrToIntDef(codActivityStr, -1);
    if codActivity <= 0 then
    begin
      dbfActivities.Next;
      Continue;
    end;

    activityDesc := Trim(dbfActivities.FieldByName('DESCRICAO').AsString);


    query.Close;
    query.SQL.Text := 'SELECT 1 FROM RAMOATIVIDADE WHERE IDRAMOATIVIDADE = :COD';
    query.ParamByName('COD').AsInteger := codActivity;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO RAMOATIVIDADE (IDRAMOATIVIDADE, DESCRICAO) ' +
        'VALUES (:COD, :DESCRICAO)';
      query.ParamByName('COD').AsInteger := codActivity;
      query.ParamByName('DESCRICAO').AsString := Copy(activityDesc, 1, 30);
      query.ExecSQL;
    end;

    dbfActivities.Next;
  end;

  trans.Commit;
  writeln('Importação de Ramo de Atividade concluída.');

  if dbfActivities.Active then dbfActivities.Close;
  FreeAndNil(dbfActivities);
end;

function Fit(const S: String; MaxLen: Integer): String;
begin
  if Length(S) > MaxLen then
    Result := Copy(S, 1, MaxLen)
  else
    Result := S;
end;

procedure ImportSuppliers(conn: TIBConnection; trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfSupp: TDbf;

  idFornecedorStr, razao, fantasia, ie, cnpj,
  telefone, celular, email, obs: String;

  idFornecedor: Integer;
  dataCadastro: TDateTime;
begin
  dbfSupp := TDbf.Create(nil);
  dbfSupp.FilePathFull := ExtractFilePath(systemPath + '/fornece.dbf');
  dbfSupp.TableName := ExtractFileName(systemPath + '/fornece.dbf');
  dbfSupp.Open;

  dbfSupp.First;
  while not dbfSupp.EOF do
  begin
    idFornecedorStr := Trim(dbfSupp.FieldByName('CODFOR').AsString);
    idFornecedor := StrToIntDef(idFornecedorStr, -1);
    if idFornecedor <= 0 then
    begin
      dbfSupp.Next;
      Continue;
    end;

    razao    := Trim(dbfSupp.FieldByName('NOME').AsString);
    fantasia := Trim(dbfSupp.FieldByName('FANTASIA').AsString);
    ie       := Trim(dbfSupp.FieldByName('INSC_EST').AsString);
    cnpj     := Trim(dbfSupp.FieldByName('CGC').AsString);
    telefone := Trim(dbfSupp.FieldByName('TEL').AsString);
    celular  := Trim(dbfSupp.FieldByName('CONTATO').AsString);
    email    := Trim(dbfSupp.FieldByName('E_MAIL').AsString);
    obs      := Trim(dbfSupp.FieldByName('OBS').AsString);

    if not dbfSupp.FieldByName('DATA_CADAS').IsNull then
      dataCadastro := dbfSupp.FieldByName('DATA_CADAS').AsDateTime
    else
      dataCadastro := Now;

    query.Close;
    query.SQL.Text := 'SELECT 1 FROM FORNECEDOR WHERE IDFORNECEDOR = :COD';
    query.ParamByName('COD').AsInteger := idFornecedor;
    query.Open;

    if query.IsEmpty then
    begin
      query.Close;
      query.SQL.Text :=
        'INSERT INTO FORNECEDOR(' +
        'IDFORNECEDOR, RAZAOSOCIAL, NOMEFANTASIA, INSCEST, CNPJ,' +
        'CODCLASSE, IDRAMOATIVIDADE, TELEFONE, CELULAR, EMAIL, SITE,' +
        'OBSERVACOES, TAGS, CADASTRADOEM)' +
        'VALUES (' +
        ':COD, :RAZAO, :FANTASIA, :IE, :CNPJ,' +
        'NULL, NULL, :TEL, :CEL, :EMAIL, NULL,' +
        ':OBS, NULL, :CAD)';

      query.ParamByName('COD').AsInteger := idFornecedor;
      query.ParamByName('RAZAO').AsString := Fit(razao, 60);
      query.ParamByName('FANTASIA').AsString := Fit(fantasia, 60);
      query.ParamByName('IE').AsString := Fit(ie, 20);
      query.ParamByName('CNPJ').AsString := Fit(cnpj, 20);
      query.ParamByName('TEL').AsString := Fit(telefone, 17);
      query.ParamByName('CEL').AsString := Fit(celular, 17);
      query.ParamByName('EMAIL').AsString := Fit(email, 50);
      query.ParamByName('OBS').AsString := obs;
      query.ParamByName('CAD').AsDateTime := dataCadastro;

      query.ExecSQL;
    end;

    dbfSupp.Next;
  end;

  trans.Commit;

  dbfSupp.Close;
  FreeAndNil(dbfSupp);

  writeln('Importação de fornecedores concluída.');
end;

function SafeStr2(f: TField): String;
begin
  if (f = nil) then Exit('');

  if f.DataType in [ftMemo, ftWideMemo, ftBlob, ftGraphic, ftUnknown] then
    Exit('');

  try
    Result := Trim(f.AsString);
  except
    Result := '';
  end;
end;

function SafeFloat2(f: TField): Double;
var
  s: String;
begin
  if (f = nil) or f.IsNull then Exit(0);

  if f.DataType in [ftMemo, ftBlob, ftWideMemo, ftUnknown] then
    Exit(0);

  try
    Result := f.AsFloat;
  except
    try
      s := Trim(f.AsString).Replace(',', '.', [rfReplaceAll]);
      Result := StrToFloatDef(s, 0);
    except
      Result := 0;
    end;
  end;
end;

function CodigoBarrasValido(b: String): Boolean;
var
  i: Integer;
begin
  b := Trim(b);
  if b = '' then Exit(False);

  for i := 1 to Length(b) do
    if not (b[i] in ['0'..'9']) then
      Exit(False);

  Result := Length(b) in [8, 12, 13, 14];
end;

function CodigoBarrasExiste(query: TSQLQuery; barra: String): Boolean;
begin
  barra := Trim(barra);
  if barra = '' then Exit(False);

  query.Close;
  query.SQL.Text := 'SELECT 1 FROM PRODUTO WHERE CODBARRAS = :B';
  query.ParamByName('B').AsString := barra;
  query.Open;

  if not query.IsEmpty then Exit(True);

  query.Close;
  query.SQL.Text := 'SELECT 1 FROM BARRAS WHERE CODBARRAS = :B';
  query.ParamByName('B').AsString := barra;
  query.Open;

  Result := not query.IsEmpty;
end;

function GerarCodigoBarrasAuto(codProd: Integer): String;
begin
  Result := FormatFloat('00000000000000', codProd);
end;

function ResolveSituTributa(conn: TIBConnection; idICMS: Integer): String;
var
  q: TSQLQuery;
  cst, csosn: String;
begin
  Result := 'N';

  if idICMS <= 0 then Exit;

  q := TSQLQuery.Create(nil);
  try
    q.DataBase := conn;
    q.Transaction := conn.Transaction;

    q.SQL.Text :=
      'SELECT CST, CSOSN FROM ICMS WHERE ID_ICMS = :ID';
    q.ParamByName('ID').AsInteger := idICMS;
    q.Open;

    if q.IsEmpty then Exit;

    cst   := Trim(q.FieldByName('CST').AsString);
    csosn := Trim(q.FieldByName('CSOSN').AsString);

    if csosn <> '' then
    begin
      case StrToIntDef(csosn, -1) of
        101, 102, 103, 900: Result := 'T';
        300: Result := 'F';
        400: Result := 'I';
        500: Result := 'G';
      end;
      Exit;
    end;

    case StrToIntDef(cst, -1) of
      0:  Result := 'T';
      10: Result := 'F';
      20: Result := 'T';
      30: Result := 'F';
      40: Result := 'I';
      41: Result := 'N';
      50: Result := 'N';
      60: Result := 'G';
      70: Result := 'H';
      90: Result := 'T';
    end;

  finally
    q.Free;
  end;
end;

function GetAliquotaICMS(conn: TIBConnection; idICMS: Integer): Double;
var
  q: TSQLQuery;
begin
  Result := 0;

  if idICMS <= 0 then Exit;

  q := TSQLQuery.Create(nil);
  try
    q.DataBase := conn;
    q.Transaction := conn.Transaction;

    q.SQL.Text :=
      'SELECT ALIQICMS FROM ICMS WHERE ID_ICMS = :ID';
    q.ParamByName('ID').AsInteger := idICMS;
    q.Open;

    if not q.IsEmpty then
    begin
      Result := q.FieldByName('ALIQICMS').AsFloat;
    end;

  finally
    q.Free;
  end;
end;

procedure ImportProducts(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfProd: TDbf;
  codProdStr, descProd, compl, unid, ref, ncm, cest: String;
  gtinTrib, gtinProd, codGrupoStr, codMarcaStr, codCFIStr, codGradeStr: String;
  codProd, codGrupo, codMarca, codCFI, codGrade: Integer;
  precoVenda, precoCusto, precoEntrada, desconto: Double;
  estoque1, estoque2, estoque3, estoque4: Double;
  estoqueTotal, estoqueMin, estoqueMax, aliqICMS: Double;
  situTrib, codBarrasPrincipal: String;
begin
  dbfProd := TDbf.Create(nil);
  try
    dbfProd.FilePathFull := ExtractFilePath(systemPath + '/itens.dbf');
    dbfProd.TableName    := 'itens.dbf';
    dbfProd.Open;

    dbfProd.First;
    while not dbfProd.EOF do
    begin
      codProdStr := SafeStr2(dbfProd.FieldByName('CODITE'));
      codProd := StrToIntDef(codProdStr, -1);
      if codProd <= 0 then
      begin
        dbfProd.Next;
        Continue;
      end;

      descProd := SafeStr2(dbfProd.FieldByName('DESCRICAO'));
      compl    := SafeStr2(dbfProd.FieldByName('MODELO'));
      unid     := SafeStr2(dbfProd.FieldByName('UN'));
      ref      := SafeStr2(dbfProd.FieldByName('REFERENCIA'));
      ncm      := SafeStr2(dbfProd.FieldByName('NBM'));
      cest     := SafeStr2(dbfProd.FieldByName('CEST'));

      gtinTrib := SafeStr2(dbfProd.FieldByName('GTIN_TRIB'));
      gtinProd := SafeStr2(dbfProd.FieldByName('GTIN_PROD'));

      codGrupo    := StrToIntDef(SafeStr2(dbfProd.FieldByName('GRUPO')), 0);
      codMarca    := StrToIntDef(SafeStr2(dbfProd.FieldByName('CODMAR')), 0);
      codCFI      := StrToIntDef(SafeStr2(dbfProd.FieldByName('CODCFI')), 0);
      codGrade    := StrToIntDef(SafeStr2(dbfProd.FieldByName('CODGRADE')), 0);

      precoVenda   := SafeFloat2(dbfProd.FieldByName('P_VENDA'));
      precoCusto   := SafeFloat2(dbfProd.FieldByName('P_CUSTO'));
      precoEntrada := SafeFloat2(dbfProd.FieldByName('P_ENTRADA'));
      desconto     := SafeFloat2(dbfProd.FieldByName('DESCONTO'));

      estoque1 := SafeFloat2(dbfProd.FieldByName('QTDEST1'));
      estoque2 := SafeFloat2(dbfProd.FieldByName('QTDEST2'));
      estoque3 := SafeFloat2(dbfProd.FieldByName('QTDEST3'));
      estoque4 := SafeFloat2(dbfProd.FieldByName('QTDEST4'));

      estoqueTotal := estoque1 + estoque2 + estoque3 + estoque4;
      estoqueMin   := SafeFloat2(dbfProd.FieldByName('QTDMINIMA'));
      estoqueMax   := SafeFloat2(dbfProd.FieldByName('QTDMAXIMA'));

      situTrib := ResolveSituTributa(conn, codCFI);
      aliqICMS := GetAliquotaICMS(conn, codCFI);

      codBarrasPrincipal := '';

      if CodigoBarrasValido(ref) and
         (not CodigoBarrasExiste(query, ref)) then
        codBarrasPrincipal := ref;

      if (codBarrasPrincipal = '') and
         CodigoBarrasValido(gtinProd) and
         (not CodigoBarrasExiste(query, gtinProd)) then
        codBarrasPrincipal := gtinProd;

      if (codBarrasPrincipal = '') and
         CodigoBarrasValido(gtinTrib) and
         (not CodigoBarrasExiste(query, gtinTrib)) then
        codBarrasPrincipal := gtinTrib;

      if (codBarrasPrincipal = '') then
        codBarrasPrincipal := GerarCodigoBarrasAuto(codProd);

      codBarrasPrincipal := Copy(codBarrasPrincipal, 1, 20);

      query.Close;
      query.SQL.Text := 'SELECT 1 FROM PRODUTO WHERE CODPRODUTO = :COD';
      query.ParamByName('COD').AsInteger := codProd;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO PRODUTO('+
          'CODPRODUTO, CODEXTERNO, CODBARRAS, DESCRICAO, COMPLEMENTO, UN,'+
          'PRECO_VENDA, PRECO_CUSTO, PRECO_ENTRADA, DESCONTO,'+
          'CODGRUPO, CODMARCA, ID_ICMS, SITU_TRIBUTA, NCM, CEST,'+
          'QTD_ESTOQUE, ESTOQUE_MIN, ESTOQUE_MAX, CONTROLA_ESTOQUE,'+
          'GTINTRIB, GTINCOM, CODGRADE, ICMS, FLAG, ALTERADO) '+
          'VALUES ('+
          ':COD, :CODEXT, :CODBARRAS, :DESC, :COMPL, :UN,'+
          ':PVENDA, :PCUSTO, :PENTRADA, :DESCONTO,'+
          ':CODGRUPO, :CODMARCA, :IDICMS, :SITUTRIB, :NCM, :CEST,'+
          ':QTEST, :ESTMIN, :ESTMAX, 0,'+
          ':GTINTRIB, :GTINCOM, :CODGRADE, :ICMS, 0, :ALTERADO)';

        query.ParamByName('COD').AsInteger := codProd;
        query.ParamByName('CODEXT').AsString := codProdStr;
        query.ParamByName('CODBARRAS').AsString := codBarrasPrincipal;
        query.ParamByName('DESC').AsString := Copy(descProd, 1, 60);
        query.ParamByName('COMPL').AsString := Copy(compl, 1, 60);
        query.ParamByName('UN').AsString := Copy(unid, 1, 4);
        query.ParamByName('PVENDA').AsFloat := precoVenda;
        query.ParamByName('PCUSTO').AsFloat := precoCusto;
        query.ParamByName('PENTRADA').AsFloat := precoEntrada;
        query.ParamByName('DESCONTO').AsFloat := desconto;
        query.ParamByName('CODGRUPO').AsInteger := codGrupo;
        query.ParamByName('CODMARCA').AsInteger := codMarca;
        query.ParamByName('IDICMS').AsInteger := codCFI;
        query.ParamByName('SITUTRIB').AsString := situTrib;
        query.ParamByName('NCM').AsString := Copy(ncm, 1, 10);
        query.ParamByName('CEST').AsString := Copy(cest, 1, 7);
        query.ParamByName('QTEST').AsFloat := estoqueTotal;
        query.ParamByName('ESTMIN').AsFloat := estoqueMin;
        query.ParamByName('ESTMAX').AsFloat := estoqueMax;
        query.ParamByName('GTINTRIB').AsString := Copy(gtinTrib, 1, 20);
        query.ParamByName('GTINCOM').AsString := Copy(gtinProd, 1, 20);
        query.ParamByName('CODGRADE').AsInteger := codGrade;
        query.ParamByName('ICMS').AsFloat := aliqICMS;
        query.ParamByName('ALTERADO').AsDateTime := Now;
        query.ExecSQL;
      end;

      if CodigoBarrasValido(gtinTrib) and
        (not CodigoBarrasExiste(query, gtinTrib)) then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO BARRAS(CODBARRAS, CODPRODUTO, DESC_ACRES, PORCENTAGEM, QTDEMBALAGEM) '+
          'VALUES (:B, :COD, '''', 0, 1)';
        query.ParamByName('B').AsString := Copy(gtinTrib, 1, 20);
        query.ParamByName('COD').AsInteger := codProd;
        query.ExecSQL;
      end;

      if CodigoBarrasValido(ref) and
        (not CodigoBarrasExiste(query, ref)) then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO BARRAS(CODBARRAS, CODPRODUTO, DESC_ACRES, PORCENTAGEM, QTDEMBALAGEM) '+
          'VALUES (:B, :COD, '''', 0, 1)';
        query.ParamByName('B').AsString := Copy(ref, 1, 20);
        query.ParamByName('COD').AsInteger := codProd;
        query.ExecSQL;
      end;

      dbfProd.Next;
    end;

    trans.Commit;
    Writeln('Importação de produtos concluída.');
  finally
    if dbfProd.Active then dbfProd.Close;
    FreeAndNil(dbfProd);
  end;
end;

procedure ImportGrades(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfGrade: TDbf;
  codGradeStr, descGrade: String;
  codGrade: Integer;
begin
  dbfGrade := TDbf.Create(nil);
  try
    dbfGrade.FilePathFull := ExtractFilePath(systemPath + '/grade.dbf');
    dbfGrade.TableName    := ExtractFileName(systemPath + '/grade.dbf');
    dbfGrade.Open;

    dbfGrade.First;
    while not dbfGrade.EOF do
    begin
      codGradeStr := Trim(dbfGrade.FieldByName('CODGRADE').AsString);
      codGrade    := StrToIntDef(codGradeStr, -1);
      if codGrade <= 0 then
      begin
        dbfGrade.Next;
        Continue;
      end;

      descGrade := Trim(dbfGrade.FieldByName('DESCRICAO').AsString);

      query.Close;
      query.SQL.Text := 'SELECT 1 FROM TIPO_GRADE WHERE CODGRADE = :COD';
      query.ParamByName('COD').AsInteger := codGrade;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO TIPO_GRADE (CODGRADE, DESCRICAO, ATIVO) ' +
          'VALUES (:COD, :DESC, ''S'')';

        query.ParamByName('COD').AsInteger  := codGrade;
        query.ParamByName('DESC').AsString  := Copy(descGrade, 1, 30);

        query.ExecSQL;
      end;

      dbfGrade.Next;
    end;

    trans.Commit;
    Writeln('Importação de grades concluída.');
  finally
    if dbfGrade.Active then dbfGrade.Close;
    FreeAndNil(dbfGrade);
  end;
end;

function IfThenInt(cond: Boolean; a, b: Integer): Integer;
begin
  if cond then Result := a else Result := b;
end;

procedure ImportPlanoPagto(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfPlano: TDbf;
  codPlaStr: String;
  codPla: Integer;

  descricao: String;
  diasEntrada, diasParcela, parcelas: Integer;
  porcEntrada: Double;

  descNota, descProd: Double;
  prazoMedio: Integer;

  carteiraPnt, fpgPnt, mantemDia, permitidoEm, descProdChar: String;
begin
  dbfPlano := TDbf.Create(nil);
  try
    dbfPlano.FilePathFull := ExtractFilePath(systemPath + '/planopg.dbf');
    dbfPlano.TableName    := 'planopg.dbf';
    dbfPlano.Open;

    dbfPlano.First;
    while not dbfPlano.EOF do
    begin
      codPlaStr := Trim(dbfPlano.FieldByName('CODPLA').AsString);
      codPla    := StrToIntDef(codPlaStr, 0);
      if codPla = 0 then
      begin
        dbfPlano.Next;
        Continue;
      end;

      descricao   := Copy(Trim(dbfPlano.FieldByName('DESCRICAO').AsString), 1, 60);

      diasEntrada := dbfPlano.FieldByName('N_DIAS_ENT').AsInteger;
      diasParcela := dbfPlano.FieldByName('N_DIAS_PAR').AsInteger;
      parcelas    := dbfPlano.FieldByName('PARCELAS').AsInteger;
      porcEntrada := dbfPlano.FieldByName('VALOR_ENT').AsFloat;

      descNota    := dbfPlano.FieldByName('DESCNOTA').AsFloat;
      descProd    := dbfPlano.FieldByName('DESCPROD').AsFloat;
      prazoMedio  := dbfPlano.FieldByName('PRAZOMEDIO').AsInteger;

      carteiraPnt  := 'N';
      fpgPnt       := 'N';
      mantemDia    := 'N';
      descProdChar := 'N';
      permitidoEm  := 'V';

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM PLANOPAGTO WHERE CODPLANOPAGTO = :C';
      query.ParamByName('C').AsInteger := codPla;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO PLANOPAGTO('+
          'CODPLANOPAGTO, DESCRICAO, DIAS_ENTRADA, PORC_ENTRADA, '+
          'PARCELAS, DIAS_PARCELA, MANTEM_DIA, PRAZO_MEDIO_MAX, '+
          'DESC_MIN, DESC_MAX, ACRES_MIN, ACRES_MAX, '+
          'DESC_PROD, FPG_PNT, CARTEIRA_PNT, PERMITIDO_EM, DESCMAXPROD) '+
          'VALUES ('+
          ':COD, :DESC, :DIASENT, :PORCENTENT, '+
          ':PARC, :DIASPARC, :MANTEM, :PRAZO, '+
          ':DESCMIN, :DESCMAX, 0, 0, '+
          ':DESCPRODCHAR, :FPG, :CART, :PERMIT, :DESCMAXPROD)';

        query.ParamByName('COD').AsInteger       := codPla;
        query.ParamByName('DESC').AsString       := Copy(descricao, 1, 20);

        query.ParamByName('DIASENT').AsInteger   := diasEntrada;
        query.ParamByName('PORCENTENT').AsFloat  := porcEntrada;

        query.ParamByName('PARC').AsInteger      := parcelas;
        query.ParamByName('DIASPARC').AsInteger  := diasParcela;
        query.ParamByName('MANTEM').AsString     := mantemDia;

        query.ParamByName('PRAZO').AsInteger     := prazoMedio;

        query.ParamByName('DESCMIN').AsFloat     := 0;
        query.ParamByName('DESCMAX').AsFloat     := descNota;

        query.ParamByName('DESCPRODCHAR').AsString := descProdChar;
        query.ParamByName('FPG').AsString          := fpgPnt;
        query.ParamByName('CART').AsString         := carteiraPnt;
        query.ParamByName('PERMIT').AsString       := permitidoEm;

        query.ParamByName('DESCMAXPROD').AsFloat := descProd;

        query.ExecSQL;
      end;

      dbfPlano.Next;
    end;

    trans.Commit;
    Writeln('Importação de planos concluída!');
  finally
    if dbfPlano.Active then dbfPlano.Close;
    FreeAndNil(dbfPlano);
  end;
end;

procedure ImportPreVenda(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfPrev: TDbf;
  codOrcStr: String;
  codPrev: Integer;

  codCliStr: String;
  codCli: Integer;

  nomeCli, docCli: String;

  subtotal, desconto, acrescimo: Double;
  dtEmis: TDateTime;
  obs: String;
begin
  dbfPrev := TDbf.Create(nil);
  try
    dbfPrev.FilePathFull := ExtractFilePath(systemPath + '/orcament.dbf');
    dbfPrev.TableName    := 'orcament.dbf';
    dbfPrev.Open;

    dbfPrev.First;
    while not dbfPrev.EOF do
    begin
      codOrcStr := Trim(dbfPrev.FieldByName('CODORC').AsString);
      codPrev   := StrToIntDef(codOrcStr, 0);

      if codPrev = 0 then
      begin
        dbfPrev.Next;
        Continue;
      end;

      codCliStr := Trim(dbfPrev.FieldByName('CODCLI').AsString);
      codCli    := StrToIntDef(codCliStr, 0);

      nomeCli := '';
      docCli  := '';

      if codCli > 0 then
      begin
        query.Close;
        query.SQL.Text :=
          'SELECT NOME, CPF_CNPJ AS DOCUMENTO FROM CLIENTE WHERE CODCLIENTE = :C';
        query.ParamByName('C').AsInteger := codCli;
        query.Open;

        if not query.IsEmpty then
        begin
          nomeCli := Trim(query.FieldByName('NOME').AsString);
          docCli  := Trim(query.FieldByName('DOCUMENTO').AsString);
        end;
      end;

      subtotal   := dbfPrev.FieldByName('TOTAL').AsFloat;
      desconto   := dbfPrev.FieldByName('DESCONTO').AsFloat;
      acrescimo  := dbfPrev.FieldByName('ACRESCIMO').AsFloat;
      obs        := Trim(dbfPrev.FieldByName('OBS').AsString);

      dtEmis := dbfPrev.FieldByName('DATA_EMIS').AsDateTime;
      if dtEmis = 0 then
        dtEmis := Now;

      query.Close;
      query.SQL.Text := 'SELECT 1 FROM PREVENDA WHERE CODPREVENDA = :C';
      query.ParamByName('C').AsInteger := codPrev;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO PREVENDA ('+
          'CODLOJA, CODPREVENDA, DATAHORA_INICIO, DATAHORA_FIM, '+
          'OBSERVACOES, CODCLIENTE, DOCUMENTO_CLI, NOME_CLI, '+
          'SUBTOTAL, DESCONTO_ACRESCIMO, CODPLANOPAGTO, CANCELADO, '+
          'ENTREGAR, IDUSER, CODTERMINAL, CODTURNO, COO, STATUS_PEDIDO, '+
          'IDMESA, TAXA_SERVICO, SOLICITAR_CPF_CNPJ, CODOPERACAO, '+
          'CODEXTERNO) '+
          'VALUES ('+
          '1, :COD, :INI, :FIM, :OBS, :CLI, :DOC, :NOMECLI, '+
          ':SUBT, :DESCACR, 1, ''N'', '+
          '''N'', 1, 1, 1, 0, NULL, '+
          '0, 0, ''N'', ''VD'', '+
          'NULL)';

        query.ParamByName('COD').AsInteger := codPrev;

        query.ParamByName('INI').AsDateTime := dtEmis;
        query.ParamByName('FIM').AsDateTime := dtEmis;

        query.ParamByName('OBS').AsString     := Copy(obs, 1, 200);
        query.ParamByName('CLI').AsInteger    := codCli;
        query.ParamByName('DOC').AsString     := docCli;
        query.ParamByName('NOMECLI').AsString := Copy(nomeCli, 1, 60);

        query.ParamByName('SUBT').AsFloat    := subtotal;
        query.ParamByName('DESCACR').AsFloat := (desconto - acrescimo);

        query.ExecSQL;
      end;

      dbfPrev.Next;
    end;

    trans.Commit;
    Writeln('Importação de PREVENDA concluída.');

  finally
    if dbfPrev.Active then dbfPrev.Close;
    dbfPrev.Free;
  end;
end;

procedure ImportPreItem(
  conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery;
  systemPath: String);
var
  dbf: TDbf;
  codPrevStr, codProdStr: String;
  codPrevenda, codProd: Integer;
  qtd, precoUnit, desconto: Double;
  seq: Integer;
  codCFI: Integer;
  situTrib: String;
begin
  dbf := TDbf.Create(nil);
  try
    dbf.FilePathFull := ExtractFilePath(systemPath + '/oitens.dbf');
    dbf.TableName    := 'oitens.dbf';
    dbf.Open;

    dbf.First;
    while not dbf.EOF do
    begin
      codPrevStr := SafeStr2(dbf.FieldByName('CODORC'));
      codPrevenda := StrToIntDef(codPrevStr, -1);

      codProdStr := SafeStr2(dbf.FieldByName('CODITE'));
      codProd := StrToIntDef(codProdStr, -1);

      if (codPrevenda <= 0) or (codProd <= 0) then
      begin
        dbf.Next;
        Continue;
      end;

      query.Close;
      query.SQL.Text := 'SELECT 1 FROM PREVENDA WHERE CODPREVENDA = :C';
      query.ParamByName('C').AsInteger := codPrevenda;
      query.Open;

      if query.IsEmpty then
      begin
        dbf.Next;
        Continue;
      end;

      query.Close;
      query.SQL.Text := 'SELECT ID_ICMS FROM PRODUTO WHERE CODPRODUTO = :C';
      query.ParamByName('C').AsInteger := codProd;
      query.Open;

      if query.IsEmpty then
      begin
        dbf.Next;
        Continue;
      end;

      codCFI := query.FieldByName('ID_ICMS').AsInteger;
      situTrib := ResolveSituTributa(conn, codCFI);

      qtd        := SafeFloat2(dbf.FieldByName('QTD_MOV'));
      precoUnit  := SafeFloat2(dbf.FieldByName('PRECOUNIT'));
      desconto   := SafeFloat2(dbf.FieldByName('DESCONTO'));

      query.Close;
      query.SQL.Text :=
        'SELECT COALESCE(MAX(SEQUENCIA),0) FROM PRE_ITEM WHERE CODPREVENDA = :C';
      query.ParamByName('C').AsInteger := codPrevenda;
      query.Open;
      seq := query.Fields[0].AsInteger + 1;

      query.Close;
      query.SQL.Text :=
        'INSERT INTO PRE_ITEM ('+
        'CODLOJA, CODPREVENDA, SEQUENCIA, CODPRODUTO, QTD, PRECO_UNITARIO,'+
        'DESCONTO_ACRESCIMO, SITU_TRIBUTA, ICMS, CANCELADO, IMPRESSO) '+
        'VALUES ('+
        '1, :PREV, :SEQ, :PROD, :QTD, :PRECO, :DESC, :SIT, 0, ''N'', ''N'')';

      query.ParamByName('PREV').AsInteger := codPrevenda;
      query.ParamByName('SEQ').AsInteger  := seq;
      query.ParamByName('PROD').AsInteger := codProd;
      query.ParamByName('QTD').AsFloat    := qtd;
      query.ParamByName('PRECO').AsFloat  := precoUnit;
      query.ParamByName('DESC').AsFloat   := desconto;
      query.ParamByName('SIT').AsString   := situTrib;

      query.ExecSQL;

      dbf.Next;
    end;

    trans.Commit;
    Writeln('Importação de PRE_ITEM concluída.');
  finally
    if dbf.Active then dbf.Close;
    dbf.Free;
  end;
end;

procedure ImportTerminais(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);
var
  dbfTer: TDbf;
  codTerStr, descTer: String;
  codTer: Integer;
begin
  dbfTer := TDbf.Create(nil);
  try
    dbfTer.FilePathFull := ExtractFilePath(systemPath + '/terminal.dbf');
    dbfTer.TableName    := 'terminal.dbf';
    dbfTer.Open;

    dbfTer.First;
    while not dbfTer.EOF do
    begin
      codTerStr := Trim(dbfTer.FieldByName('TERMINAL').AsString);
      codTer    := StrToIntDef(codTerStr, 0);

      if codTer <= 0 then
      begin
        dbfTer.Next;
        Continue;
      end;

      descTer := Trim(dbfTer.FieldByName('DESCRICAO').AsString);
      if descTer = '' then
        descTer := 'Terminal ' + codTerStr;

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM TERMINAL WHERE CODTERMINAL = :COD';
      query.ParamByName('COD').AsInteger := codTer;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO TERMINAL ('+
          'CODLOJA, CODTERMINAL, CODEXTERNO, DESCRICAO, ESTADO, ATIVO, TIPO) '+
          'VALUES (1, :COD, :COD, :DESC, 1, 1, 0)';

        query.ParamByName('COD').AsInteger  := codTer;
        query.ParamByName('DESC').AsString := Copy(descTer, 1, 60);

        query.ExecSQL;
      end;

      dbfTer.Next;
    end;

    trans.Commit;
    writeln('Importação de TERMINAIS concluída.');
  finally
    if dbfTer.Active then dbfTer.Close;
    dbfTer.Free;
  end;
end;

procedure ImportTurno(conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery; systemPath: String);

  function SafeDate(f: TField): TDate;
  var s: string;
  begin
    s := Trim(f.AsString);
    if (s = '') then
      Exit(Date);
    if not TryStrToDate(s, Result) then
      Result := Date;
  end;

  function SafeTimeStr(s: String): TTime;
  begin
    s := Trim(s);
    if (s = '') then
      Exit(0);
    if not TryStrToTime(s, Result) then
      Result := 0;
  end;

var
  dbfTurno: TDbf;

  codLoja, codTer, codTur: Integer;
  abertoEm, fechadoEm: TDateTime;

  dataA, dataF: TDate;
  horaA, horaF: String;

  codOperador, crz, cro: Integer;
begin
  dbfTurno := TDbf.Create(nil);
  try
    dbfTurno.FilePathFull := ExtractFilePath(systemPath + '/turno.dbf');
    dbfTurno.TableName    := 'turno.dbf';
    dbfTurno.Open;

    dbfTurno.First;
    while not dbfTurno.EOF do
    begin
      codLoja := 1;

      codTer := StrToIntDef(Trim(dbfTurno.FieldByName('CODCAI').AsString), 1);

      codTur := StrToIntDef(Trim(dbfTurno.FieldByName('TURNO').AsString), 0);
      if codTur = 0 then
      begin
        dbfTurno.Next;
        Continue;
      end;

      dataA := SafeDate(dbfTurno.FieldByName('DATA_ABER'));
      horaA := Trim(dbfTurno.FieldByName('HORA_ABER').AsString);
      abertoEm := dataA + SafeTimeStr(horaA);

      dataF := SafeDate(dbfTurno.FieldByName('DATA_FECHA'));
      horaF := Trim(dbfTurno.FieldByName('HORA_FECHA').AsString);
      fechadoEm := dataF + SafeTimeStr(horaF);

      codOperador := 1;
      crz := 0;
      cro := 0;

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM TURNO ' +
        'WHERE CODLOJA = :L AND CODTERMINAL = :T AND CODTURNO = :U';
      query.ParamByName('L').AsInteger := codLoja;
      query.ParamByName('T').AsInteger := codTer;
      query.ParamByName('U').AsInteger := codTur;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO TURNO ('+
          'CODLOJA, CODTERMINAL, CODTURNO, '+
          'ABERTO_EM, ABERTO_POR, '+
          'CODOPERADOR, CRZ, CRO) '+
          'VALUES ('+
          ':L, :T, :U, '+
          ':AEM, :FEM, '+
          ':OP, :CRZ, :CRO)';

        query.ParamByName('L').AsInteger := codLoja;
        query.ParamByName('T').AsInteger := codTer;
        query.ParamByName('U').AsInteger := codTur;

        query.ParamByName('AEM').AsDateTime := abertoEm;
        query.ParamByName('FEM').AsDateTime := fechadoEm;

        query.ParamByName('OP').AsInteger  := codOperador;
        query.ParamByName('CRZ').AsInteger := crz;
        query.ParamByName('CRO').AsInteger := cro;

        query.ExecSQL;
      end;

      dbfTurno.Next;
    end;

    trans.Commit;
    Writeln('Importação de TURNO concluída.');
  finally
    if dbfTurno.Active then dbfTurno.Close;
    dbfTurno.Free;
  end;
end;

function SanitizeHour(const S: String): TDateTime;
var
  aux: String;
  HH, MM: Integer;
begin
  aux := Trim(S);

  aux := StringReplace(aux, ':', '', [rfReplaceAll]);
  aux := StringReplace(aux, ' ', '', [rfReplaceAll]);

  aux := Trim(aux);

  if aux = '' then
    Exit(EncodeTime(0,0,0,0));

  if Length(aux) = 1 then aux := '0' + aux + '00';
  if Length(aux) = 2 then aux := aux + '00';
  if Length(aux) = 3 then aux := '0' + aux;

  aux := Copy(aux, 1, 4);

  HH := StrToIntDef(Copy(aux,1,2), 0);
  MM := StrToIntDef(Copy(aux,3,2), 0);

  if HH > 23 then HH := 0;
  if MM > 59 then MM := 0;

  Result := EncodeTime(HH, MM, 0, 0);
end;

procedure ImportDocumento(conn: TIBConnection;
  trans: TSQLTransaction; query: TSQLQuery; systemPath: String);
var
  dbfSai: TDbf;

  codDoc, codCli, codPla: Integer;
  codTer, codTur: Integer;
  subtotal, desconto, acrescimo: Double;

  dt: TDateTime;
  dtStr, hrStr: String;

  nomeCli, docCli, obs: String;
begin
  dbfSai := TDbf.Create(nil);
  try
    dbfSai.FilePathFull := ExtractFilePath(systemPath + '/saida.dbf');
    dbfSai.TableName    := 'saida.dbf';
    dbfSai.Open;

    dbfSai.First;
    while not dbfSai.EOF do
    begin
      codDoc := StrToIntDef(Trim(dbfSai.FieldByName('MOVIMENTO').AsString), 0);
      if codDoc = 0 then
      begin
        dbfSai.Next;
        Continue;
      end;

      codTer := StrToIntDef(Trim(dbfSai.FieldByName('CODCAI').AsString), 1);
      codTur := StrToIntDef(Trim(dbfSai.FieldByName('TURNO').AsString), 1);

      codCli := StrToIntDef(Trim(dbfSai.FieldByName('CODCLI').AsString), 0);

      dtStr := Trim(dbfSai.FieldByName('DATA_EMIS').AsString);
      hrStr := Trim(dbfSai.FieldByName('HORA_EMIS').AsString);

      if Length(dtStr) = 8 then
      begin
        dt := EncodeDate(
                StrToIntDef(Copy(dtStr,1,4), 2000),
                StrToIntDef(Copy(dtStr,5,2), 1),
                StrToIntDef(Copy(dtStr,7,2), 1)
              );
      end
      else
        dt := Date;

      dt := dt + SanitizeHour(hrStr);

      subtotal  := dbfSai.FieldByName('VALOR').AsFloat;
      desconto  := dbfSai.FieldByName('DESCONTO').AsFloat;
      acrescimo := dbfSai.FieldByName('ACRESCIMO').AsFloat;

      obs    := Copy(Trim(dbfSai.FieldByName('OBS').AsString), 1, 200);
      codPla := StrToIntDef(Trim(dbfSai.FieldByName('CODPLA').AsString), 0);
      if codPla = 0 then
        codPla := 1;

      nomeCli := '';
      docCli  := '';

      if codCli > 0 then
      begin
        query.Close;
        query.SQL.Text :=
          'SELECT NOME, CPF_CNPJ FROM CLIENTE WHERE CODCLIENTE = :C';
        query.ParamByName('C').AsInteger := codCli;
        query.Open;

        if not query.IsEmpty then
        begin
          nomeCli := Copy(query.FieldByName('NOME').AsString, 1, 60);
          docCli  := Trim(query.FieldByName('CPF_CNPJ').AsString);
        end;
      end;

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM DOCUMENTO '+
        'WHERE CODLOJA=1 AND CODTERMINAL=:T AND CODTURNO=:U AND COO=:COO';
      query.ParamByName('T').AsInteger   := codTer;
      query.ParamByName('U').AsInteger   := codTur;
      query.ParamByName('COO').AsInteger := codDoc;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO DOCUMENTO ('+
          'CODLOJA, CODTERMINAL, CODTURNO, COO, DENOMINACAO, '+
          'DATAHORA_INICIO, DATAHORA_FIM, CODCLIENTE, NOME_CLI, DOCUMENTO_CLI, '+
          'SUBTOTAL, DESCONTO_ACRESCIMO, TOTAL_PAGO, CODPLANOPAGTO, '+
          'CANCELADO, CODOPERACAO, OBS) '+
          'VALUES ('+
          '1, :T, :U, :COO, ''VD'', '+
          ':DT, :DT, :CLI, :NOME, :DOC, '+
          ':SUBT, :DESCACR, :TOT, :PLA, '+
          '''N'', ''VD'', :OBS)';

        query.ParamByName('T').AsInteger   := codTer;
        query.ParamByName('U').AsInteger   := codTur;
        query.ParamByName('COO').AsInteger := codDoc;
        query.ParamByName('DT').AsDateTime := dt;
        query.ParamByName('CLI').AsInteger := codCli;
        query.ParamByName('NOME').AsString := nomeCli;
        query.ParamByName('DOC').AsString  := docCli;

        query.ParamByName('SUBT').AsFloat    := subtotal;
        query.ParamByName('DESCACR').AsFloat := (desconto - acrescimo);
        query.ParamByName('TOT').AsFloat     := subtotal;
        query.ParamByName('PLA').AsInteger   := codPla;
        query.ParamByName('OBS').AsString    := obs;

        query.ExecSQL;
      end;

      dbfSai.Next;
    end;

    trans.Commit;
    Writeln('Importação de DOCUMENTO concluída.');
  finally
    dbfSai.Free;
  end;
end;

procedure ImportDocItem(conn: TIBConnection;
  trans: TSQLTransaction; query: TSQLQuery; systemPath: String);
var
  dbfIt: TDbf;
  codSai, codProd, codTer, codTur, seq: Integer;
  qtd, preco, desc: Double;
  seqStr: String;
begin
  dbfIt := TDbf.Create(nil);
  try
    dbfIt.FilePathFull := ExtractFilePath(systemPath + '/nitens.dbf');
    dbfIt.TableName    := 'nitens.dbf';
    dbfIt.Open;

    dbfIt.First;
    while not dbfIt.EOF do
    begin
      codSai := StrToIntDef(Trim(dbfIt.FieldByName('MOVIMENTO').AsString), 0);
      if codSai = 0 then begin dbfIt.Next; Continue; end;

      codProd := StrToIntDef(Trim(dbfIt.FieldByName('CODITE').AsString), 0);
      if codProd = 0 then begin dbfIt.Next; Continue; end;

      codTer := StrToIntDef(Trim(dbfIt.FieldByName('CODCAI').AsString), 1);

      codTur := StrToIntDef(Trim(dbfIt.FieldByName('TURNO').AsString), 1);

      seqStr := Trim(dbfIt.FieldByName('ORDEM').AsString);
      seq    := StrToIntDef(seqStr, 0);
      if seq = 0 then seq := 1;

      qtd   := dbfIt.FieldByName('QTD_MOV').AsFloat;
      preco := dbfIt.FieldByName('PRECOUNIT').AsFloat;

      desc := 0;

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM DOC_ITEM WHERE '+
        'CODLOJA=1 AND CODTERMINAL=:T AND CODTURNO=:U AND COO=:COO AND SEQUENCIA=:S';
      query.ParamByName('T').AsInteger   := codTer;
      query.ParamByName('U').AsInteger   := codTur;
      query.ParamByName('COO').AsInteger := codSai;
      query.ParamByName('S').AsInteger   := seq;
      query.Open;

      if not query.IsEmpty then
      begin
        dbfIt.Next;
        Continue;
      end;

      query.Close;
      query.SQL.Text :=
        'INSERT INTO DOC_ITEM ('+
        'CODLOJA, CODTERMINAL, CODTURNO, COO, SEQUENCIA, '+
        'CODPRODUTO, QTD, PRECO_UNITARIO, DESCONTO_ACRESCIMO, CANCELADO) '+
        'VALUES (1, :T, :U, :COO, :SEQ, :P, :QTD, :PRECO, :DESC, ''N'')';

      query.ParamByName('T').AsInteger := codTer;
      query.ParamByName('U').AsInteger := codTur;
      query.ParamByName('COO').AsInteger := codSai;
      query.ParamByName('SEQ').AsInteger := seq;

      query.ParamByName('P').AsInteger := codProd;
      query.ParamByName('QTD').AsFloat := qtd;
      query.ParamByName('PRECO').AsFloat := preco;
      query.ParamByName('DESC').AsFloat := desc;

      query.ExecSQL;

      dbfIt.Next;
    end;

    trans.Commit;
    Writeln('DOC_ITEM importado com sucesso.');
  finally
    dbfIt.Free;
  end;
end;

procedure ImportPagamentos(
  conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery;
  systemPath: String
);
var
  dbfCaixa: TDbf;
  codDoc: Integer;
  codFp: String;
  valor: Double;
begin
  dbfCaixa := TDbf.Create(nil);
  try
    dbfCaixa.FilePathFull := ExtractFilePath(systemPath + '/caixa.dbf');
    dbfCaixa.TableName    := 'caixa.dbf';
    dbfCaixa.Open;

    dbfCaixa.First;
    while not dbfCaixa.EOF do
    begin
      codDoc := StrToIntDef(Trim(dbfCaixa.FieldByName('MOVIMENTO').AsString), 0);
      if codDoc = 0 then begin dbfCaixa.Next; Continue; end;

      codFp := Trim(dbfCaixa.FieldByName('CODHIS').AsString);
      codFp := RightStr('00' + codFp, 2);

      if (codFp = '') or (codFp = '00') then
        codFp := '01';

      valor := dbfCaixa.FieldByName('VALOR').AsFloat;

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM FORMAPAGTO WHERE CODFORMAPAGTO = :F';
      query.ParamByName('F').AsString := codFp;
      query.Open;

      if query.IsEmpty then
      begin
        query.Close;
        query.SQL.Text :=
          'INSERT INTO FORMAPAGTO ('+
          'CODFORMAPAGTO, DESCRICAO, DESCRICAO_ECF, FLAG, PERMITE_TROCO'+
          ') VALUES ('+
          ':F, :D, ''Imp System'', ''0'', ''S'')';

        query.ParamByName('F').AsString := codFp;
        query.ParamByName('D').AsString := 'FORMA ' + codFp;

        query.ExecSQL;
      end;

      query.Close;
      query.SQL.Text :=
        'SELECT 1 FROM TURNO_FPAGTO '+
        'WHERE CODLOJA=1 AND CODTERMINAL=1 AND CODTURNO=1 AND CODFPAGTO=:F';

      query.ParamByName('F').AsString := codFp;
      query.Open;

      if not query.IsEmpty then
      begin
        dbfCaixa.Next;
        Continue;
      end;

      query.Close;
      query.SQL.Text :=
        'INSERT INTO TURNO_FPAGTO ('+
        'CODLOJA, CODTERMINAL, CODTURNO, CODFPAGTO, VALOR_INFORMADO, VALOR_CALCULADO'+
        ') VALUES ('+
        '1, 1, 1, :F, :V, :V)';

      query.ParamByName('F').AsString := codFp;
      query.ParamByName('V').AsFloat  := valor;
      query.ExecSQL;

      dbfCaixa.Next;
    end;

    trans.Commit;
    Writeln('Pagamentos importados com sucesso.');
  finally
    dbfCaixa.Free;
  end;
end;

var
  conn: TIBConnection;
  trans: TSQLTransaction;
  query: TSQLQuery;
  systemPath: String;


begin
  writeln('Insira o diretório do DJSystem: ');
  readln(systemPath);

  conn := TIBConnection.Create(nil);
  trans := TSQLTransaction.Create(nil);
  query := TSQLQuery.Create(nil);

  try
    CreateEmptyDatabase;

    conn.DatabaseName := './DJPDV.FDB';
    conn.UserName := 'sysdba';
    conn.Password := 'masterkey';
    conn.CharSet := 'UTF8';
    conn.Params.Add('lc_ctype=UTF8');
    conn.Params.Add('sql_dialect=3');
    conn.Transaction := trans;

    trans.DataBase := conn;
    query.DataBase := conn;
    query.Transaction := trans;

    conn.Open;
    writeln('Banco conectado.');

    // Basicos
    //ImportGroups(conn, trans, query, systemPath);
    //ImportBrands(conn, trans, query, systemPath);
    //ImportSellers(conn, trans, query, systemPath);
    //ImportWallets(conn, trans, query, systemPath);
    //ImportTaxations(conn, trans, query, systemPath);
    //ImportCarriers(conn, trans, query, systemPath);
    //ImportPlanoPagto(conn, trans, query, systemPath);

    // Cadastros
    //ImportClients(conn, trans, query, systemPath);
    //ImportActivities(conn, trans, query, systemPath);
    //ImportSuppliers(conn, trans, query, systemPath);
    //ImportGrades(conn, trans, query, systemPath);
    //ImportProducts(conn, trans, query, systemPath);

    // Documentos
    //ImportPreVenda(conn, trans, query, systemPath);
    //ImportPreItem(conn, trans, query, systemPath);
    //ImportTerminais(conn, trans, query, systemPath);
    //ImportTurno(conn, trans, query, systemPath);
    //ImportDocumento(conn, trans, query, systemPath);
    //ImportDocItem(conn, trans, query, systemPath);
    //ImportPagamentos(conn, trans, query, systemPath);



  finally
    FreeAndNil(query);
    FreeAndNil(trans);
    if conn.Connected then conn.Close;
    FreeAndNil(conn);
  end;

  readln;
end.

