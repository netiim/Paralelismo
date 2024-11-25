using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using System.Diagnostics;
using Aprendendo;

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
