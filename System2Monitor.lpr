program System2Monitor;
{$codepage UTF8}

uses
  IBConnection, SQLDB, DB, dbf, SysUtils, Windows, Classes, Contnrs;

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

    // CST agora é COD_CONTAB
    cst := Trim(dbfFile.FieldByName('COD_CONTAB').AsString);

    // Código da tributação
    codTaxationStr := Trim(dbfFile.FieldByName('CODCFI').AsString);
    codTaxation := StrToIntDef(codTaxationStr, -1);

    modbc := StrToIntDef(Trim(dbfFile.FieldByName('MODBC').AsString), 0);
    modbCST := StrToIntDef(Trim(dbfFile.FieldByName('MODBCST').AsString), 0);
    csosn := StrToIntDef(Trim(dbfFile.FieldByName('CSOSN').AsString), 0);

    // Base ICMS = fator_base * 100
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

    // ORIGEM = primeiro dígito do CST
    if Length(cst) >= 1 then
      item.Origem := StrToIntDef(Copy(cst, 1, 1), 0)
    else
      item.Origem := 0;

    // CST = últimos 2 dígitos
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
    query.ParamByName('CODV').AsInteger := codTra;  // mesmo código do DBF
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

    // Cadastros
    //ImportClients(conn, trans, query, systemPath);
    ImportActivities(conn, trans, query, systemPath);


  finally
    FreeAndNil(query);
    FreeAndNil(trans);
    if conn.Connected then conn.Close;
    FreeAndNil(conn);
  end;

  readln;
end.

