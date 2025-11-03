const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const { Parser } = require('json2csv');
require('dotenv').config();


const getYesterday = () => {
  const date = new Date();
  date.setDate(date.getDate() - 1);
  return date.toISOString().split('T')[0];
};

const getSevenDaysAgo = () => {
  const date = new Date();
  date.setDate(date.getDate() - 7);
  return date.toISOString().split('T')[0];
};

const dataInicial = process.env.DATA_INICIAL || getSevenDaysAgo();
const dataFinal = process.env.DATA_FINAL || getYesterday();

const PERIODO_INICIAL = `${dataInicial}T00:00:00`;
const PERIODO_FINAL = `${dataFinal}T23:59:59`;

const CHUNK_SIZE = 500; 


const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY;
const argusApiToken = process.env.ARGUS_API_TOKEN;

if (!supabaseUrl || !supabaseServiceKey || !argusApiToken) {
  console.error('❌ Erro: SUPABASE_URL, SUPABASE_SERVICE_KEY e ARGUS_API_TOKEN são obrigatórios.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

const ENDPOINTS_CONFIG = [
  {
    name: 'tabulacoesdetalhadas',
    bucket: 'tabulacoesdetalhadas-Argus',
    url: 'https://argus.app.br/apiargus/report/tabulacoesdetalhadas',
    dataField: 'tabulacoes',
    idCampanha: 1,
    sqlTable: 'argus_tabulacoes_duplicate', // <-- NOVO: Tabela SQL de destino
  },
  {
    name: 'ligacoesdetalhadas',
    bucket: 'ligacoesdetalhadas-Argus',
    url: 'https://argus.app.br/apiargus/report/ligacoesdetalhadas',
    dataField: 'ligacoesDetalhadas',
    idCampanha: 1,
    sqlTable: null, // <-- NOVO: null = não fazer upload para SQL
  },
];


async function fetchPaginatedData(endpointConfig) {
  const { url, dataField, idCampanha, name } = endpointConfig;

  let allRecords = [];
  let ultimoId = 0;
  let endOfTable = false;
  let pageCount = 1;

  console.log(`\n📊 Iniciando extração do endpoint "${name}" (${dataField}) para o período ${dataInicial} → ${dataFinal}`);

  const headers = { 'Token-Signature': argusApiToken };

  do {
    try {
      console.log(`🔎 Página ${pageCount} (ultimoId: ${ultimoId})...`);

      const body = {
        idCampanha,
        periodoInicial: PERIODO_INICIAL,
        periodoFinal: PERIODO_FINAL,
        ultimoId,
      };

      const response = await axios.post(url, body, { headers });
      const data = response.data;

      if (data && data.codStatus === 1 && data[dataField]?.length > 0) {
        allRecords.push(...data[dataField]);
        console.log(`  → ${data.qtdeRegistros} registros encontrados (total: ${allRecords.length})`);

        ultimoId = data.idProxPagina;
        endOfTable = data.endOfTable || ultimoId === 0;
        pageCount++;
      } else {
        console.warn(`⚠️ Resposta vazia ou falha: ${data?.descStatus || 'Sem descrição'}`);
        endOfTable = true;
      }
    } catch (err) {
      const msg = err.response
        ? `Status: ${err.response.status} - ${JSON.stringify(err.response.data)}`
        : err.message;
      console.error(`❌ Erro ao buscar página ${pageCount}: ${msg}`);
      endOfTable = true; // Para o loop em caso de erro
    }
  } while (!endOfTable);

  console.log(`✅ Extração finalizada (${name}): ${allRecords.length} registros obtidos.`);
  return allRecords;
}


function convertJsonToCsv(jsonData) {
  if (!jsonData?.length) {
    console.log('Nenhum dado para converter para CSV.');
    return null;
  }

  try {
    const parser = new Parser();
    return parser.parse(jsonData);
  } catch (err) {
    console.error('Erro ao converter JSON para CSV:', err);
    return null;
  }
}

// ALTERADO: Função renomeada (era uploadToSupabase)
async function uploadCsvToStorage(bucketName, fileName, fileContent) {
  console.log(`\n🚀 Enviando arquivo CSV "${fileName}" para bucket "${bucketName}"...`);

  const { error } = await supabase.storage
    .from(bucketName)
    .upload(fileName, fileContent, {
      contentType: 'text/csv',
      upsert: true,
    });

  if (error) {
    console.error('Erro no upload do CSV:', error.message);
  } else {
    console.log(`✅ Upload do CSV concluído: ${bucketName}/${fileName}`);
  }
}


async function uploadJsonToTable(tableName, jsonData) {
  console.log(`\n💾 Iniciando upload de ${jsonData.length} registros para a tabela SQL "${tableName}"...`);
  
  for (let i = 0; i < jsonData.length; i += CHUNK_SIZE) {
    const chunk = jsonData.slice(i, i + CHUNK_SIZE);
    const chunkNumber = (i / CHUNK_SIZE) + 1;
    const totalChunks = Math.ceil(jsonData.length / CHUNK_SIZE);

    console.log(`  -> Enviando chunk ${chunkNumber}/${totalChunks} (${chunk.length} registros)...`);

    const { error }_ = await supabase
      .from(tableName)
      .insert(chunk); 
      // .upsert(chunk, { onConflict: 'sua_coluna_de_conflito' });

    if (error) {
      console.error(`❌ Erro ao inserir o chunk ${chunkNumber}:`, error.message);
      // Decide se quer parar o processo ou apenas logar e continuar
      // Por segurança, vamos parar o upload deste endpoint:
      throw new Error(`Falha no upload do chunk ${chunkNumber} para ${tableName}.`);
    }
  }
  console.log(`✅ Upload para a tabela "${tableName}" concluído.`);
}



async function main() {
  const selectedEndpoint = process.env.ENDPOINT || 'all';
  
  const endpointsToRun = 
    selectedEndpoint === 'all'
      ? ENDPOINTS_CONFIG
      : ENDPOINTS_CONFIG.filter(e => e.name === selectedEndpoint);

  if (endpointsToRun.length === 0) {
    console.error(`❌ Endpoint inválido: ${selectedEndpoint}`);
    process.exit(1);
  }

  for (const endpoint of endpointsToRun) {
    console.log(`\n--- Processando Endpoint: ${endpoint.name} ---`);
    try {
      // 1. Buscar dados da API Argus
      const data = await fetchPaginatedData(endpoint);
      
      if (data.length > 0) {
        
        const csv = convertJsonToCsv(data);
        if (csv) {
          const fileName = `${endpoint.name}_${dataInicial}_ate_${dataFinal}_${Date.now()}.csv`;
          await uploadCsvToStorage(endpoint.bucket, fileName, csv);
        }

        if (endpoint.sqlTable) {
          await uploadJsonToTable(endpoint.sqlTable, data);
        } else {
          console.log(`\nℹ️ Upload para SQL não configurado para o endpoint "${endpoint.name}". Pulando.`);
        }

      } else {
        console.log(`⚠️ Nenhum dado retornado para ${endpoint.name}. Pulando uploads.`);
      }
    } catch (err) {
      // Se um endpoint falhar, loga o erro e continua para o próximo
      console.error(`\n❌❌ ERRO GERAL no processamento do endpoint "${endpoint.name}": ${err.message}`);
    }
    console.log(`--- Finalizado Endpoint: ${endpoint.name} ---`);
  }

  console.log('\n🏁 Processo finalizado.');
}

main();
