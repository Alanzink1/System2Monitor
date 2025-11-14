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

    item.Multa := multa;        // <<<<<< CORREÇÃO 1
    item.JurosDia := jurosDia;  // <<<<<< CORREÇÃO 2

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

    //ImportGroups(conn, trans, query, systemPath);
    //ImportBrands(conn, trans, query, systemPath);
    //ImportSellers(conn, trans, query, systemPath);
    ImportWallets(conn, trans, query, systemPath);

  finally
    FreeAndNil(query);
    FreeAndNil(trans);
    if conn.Connected then conn.Close;
    FreeAndNil(conn);
  end;

  readln;
end.

