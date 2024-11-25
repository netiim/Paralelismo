using CsvHelper.Configuration;
using CsvHelper;
using System.Data;
using System.Globalization;
using System.Diagnostics;

namespace Aprendendo
{
    public static class ModificadorDataTableComTask
    {
        public static void ValidarHipotese()
        {
            // Inicia o cronômetro para medir o tempo total
            Stopwatch stopwatch = Stopwatch.StartNew();

            // Inicia o cronômetro para medir o tempo de carregamento do DataTable
            Stopwatch loadTime = Stopwatch.StartNew();
            DataTable table = ModificadorDataTableComTask.LoadCsvToDataTable("./rais-2021.csv");
            loadTime.Stop();
            Console.WriteLine($"Tempo para carregar o DataTable: {loadTime.ElapsedMilliseconds}ms");

            // Inicia o cronômetro para medir o tempo de modificação da coluna com Task
            Stopwatch modifyTimeParalelo = Stopwatch.StartNew();
            ModificadorDataTableComTask.ModificarColunaDataTableComParalelismo(table, "Nome da região");
            modifyTimeParalelo.Stop();
            Console.WriteLine($"Tempo para modificar com Paralelismo a coluna 'Nome da região': {modifyTimeParalelo.ElapsedMilliseconds}ms");

            //Inicia o cronômetro para medir o tempo de modificação da coluna da forma tradicional
            Stopwatch modifyTime = Stopwatch.StartNew();
            ModificadorDataTableComTask.ModificarColunaDataTable(table, "Nome da região");
            modifyTime.Stop();
            Console.WriteLine($"Tempo para modificar a coluna 'Nome da região': {modifyTime.ElapsedMilliseconds}ms");

            // Finaliza o cronômetro total
            stopwatch.Stop();
            Console.WriteLine($"Tempo total: {stopwatch.ElapsedMilliseconds}ms");
        }

        public static DataTable LoadCsvToDataTable(string filePath)
        {
            var dataTable = new DataTable();
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true, // Confirma que o arquivo CSV tem cabeçalho
                Delimiter = ";", // Define o delimitador correto (ponto e vírgula)
                BadDataFound = null // Ignora dados problemáticos
            };

            try
            {
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader, config))
                {
                    using (var dr = new CsvDataReader(csv))
                    {
                        dataTable.Load(dr);
                    }
                }
            }
            catch (HeaderValidationException ex)
            {
                Console.WriteLine($"Erro ao validar cabeçalho: {ex.Message}");
            }
            catch (BadDataException ex)
            {
                Console.WriteLine($"Erro de dados ruins: {ex.Message}");
            }

            return dataTable;
        }
        public static void ModificarColunaDataTableComParalelismo(DataTable table, string coluna)
        {
            if (!table.Columns.Contains(coluna))
            {
                Console.WriteLine($"Coluna {coluna} não encontrada!");
                return;
            }

            List<DataTable> partitions = DivideDataTable(table);

            ProcessaTaskAlteracaoDataTable(partitions, coluna);

            MesclarResultadosPartitionsComTabelaBase(table, partitions);
        }

        private static void MesclarResultadosPartitionsComTabelaBase(DataTable table, List<DataTable> partitions)
        {
            table.Rows.Clear();
            foreach (var partition in partitions)
            {
                foreach (DataRow row in partition.Rows)
                {
                    table.ImportRow(row);
                }
            }
        }

        private static void ProcessaTaskAlteracaoDataTable(List<DataTable> partitions, string coluna)
        {
            List<Task> tasks = new List<Task>();
            foreach (var partition in partitions)
            {
                partition.Columns[coluna].ReadOnly = false;
                tasks.Add(Task.Run(() =>
                {
                    for (int iter = 0; iter < 100; iter++)
                    {
                        foreach (DataRow row in partition.Rows)
                        {
                            row[coluna] = "teste";
                        }
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private static List<DataTable> DivideDataTable(DataTable table)
        {
            int totalRows = table.Rows.Count;
            int partitionSize = (int)Math.Ceiling(totalRows / 8.0);

            List<DataTable> partitions = new List<DataTable>();
            for (int i = 0; i < 8; i++)
            {
                int start = i * partitionSize;
                int end = Math.Min(start + partitionSize, totalRows);

                DataTable partition = table.Clone(); // Clona a estrutura do DataTable original
                for (int rowIndex = start; rowIndex < end; rowIndex++)
                {
                    partition.ImportRow(table.Rows[rowIndex]); // Copia as linhas para o novo DataTable
                }
                partitions.Add(partition);
            }

            return partitions;
        }

        public static void ModificarColunaDataTable(DataTable table, string coluna)
        {
            if (!table.Columns.Contains(coluna))
            {
                Console.WriteLine($"Coluna {coluna} não encontrada!");
                return;
            }
            table.Columns[coluna].ReadOnly = false;
            for (int i = 0; i < 100; i++)
            {
                foreach (DataRow row in table.Rows)
                {
                    row[coluna] = "teste";
                }
            }

        }
    }
}
