/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
 define(['N/format', 'N/record', 'N/search'],
 /**
* @param{format} format
* @param{record} record
* @param{search} search
*/
 (format, record, search) => {
     /**
      * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
      * @param {Object} inputContext
      * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
      *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
      * @param {Object} inputContext.ObjectRef - Object that references the input data
      * @typedef {Object} ObjectRef
      * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
      * @property {string} ObjectRef.type - Type of the record instance that contains the input data
      * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
      * @since 2015.2
      */

     const getInputData = (inputContext) => {
         try {
             //Obtener los clientes que tienen aviso o programado para generar su oportunidad
             let customerSearchObj = search.create({
                 type: "customer",
                 filters:
                     [
                         ["stage", "anyof", "CUSTOMER"],
                         "AND",
                         ["custentityptg_tipodecontacto_", "anyof", "2", "4"]
                     ],
                 columns:
                     [
                         search.createColumn({ name: "internalid", label: "Internal ID" }),
                         search.createColumn({ name: "custentity_ptg_periododecontacto_", label: "PTG - Periodo de contacto:" }),
                         search.createColumn({ name: "custentity_ptg_entrelas_", label: "PTG - Entre las:" }),
                         search.createColumn({ name: "custentity_ptg_ylas_", label: "PTG - Y las_" }),
                         search.createColumn({ name: "custentityptg_tipodecontacto_", label: "PTG - Tipo de Contacto:" }),
                         search.createColumn({ name: "custentity_ptg_lunes_", label: "PTG - Lunes" }),
                         search.createColumn({ name: "custentity_ptg_martes_", label: "PTG - Martes" }),
                         search.createColumn({ name: "custentity_ptg_miercoles_", label: "PTG - Miercoles" }),
                         search.createColumn({ name: "custentity_ptg_jueves_", label: "PTG - Jueves" }),
                         search.createColumn({ name: "custentity_ptg_viernes_", label: "PTG - Viernes" }),
                         search.createColumn({ name: "custentity_ptg_sabado_", label: "PTG - Sabado" }),
                         search.createColumn({ name: "custentity_ptg_domingo_", label: "PTG - Domingo" }),
                         search.createColumn({ name: "custentity_ptg_tipodeservicio_", label: "PTG - Tipo de Servicio" }),
                         search.createColumn({ name: "subsidiary", label: "Primary Subsidiary" }),
                         search.createColumn({ name: "custentity_ptg_articulo_frecuente", label: "PTG - ARTÍCULO FRECUENTE" }),
                         search.createColumn({ name: "custentity_ptg_cantidad_frecuente_lt_cil", label: "PTG - CANTIDAD FRECUENTE LT / CIL" }),
                         search.createColumn({ name: "custentity_ptg_tipodecliente_", label: "PTG - Tipo de cliente" }),
                         search.createColumn({ name: "custentity_ptg_alianza_comercial_cliente", label: "PTG - ALIANZA COMERCIAL DEL CLIENTE" }),
                         search.createColumn({ name: "address", label: "Address" }),
                         search.createColumn({
                             name: "custrecord_ptg_colonia_ruta",
                             join: "Address",
                             label: "PTG - COLONIA Y RUTA"
                         })
                     ]
             });

             let searchResultCount = customerSearchObj.runPaged().count;
             if (searchResultCount > 0) {
                 return customerSearchObj
             }
             //log.debug("customerSearchObj result count", searchResultCount);
             // customerSearchObj.run().each(function (result) {
             //     // .run().each has a limit of 4,000 results
             //     log.debug('result', result)
             //     return true;
             // });    
         } catch (error) {
             log.debug('err', error)
         }


     }

     /**
      * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
      * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
      * context.
      * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
      *     is provided automatically based on the results of the getInputData stage.
      * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
      *     function on the current key-value pair
      * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
      *     pair
      * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
      *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
      * @param {string} mapContext.key - Key to be processed during the map stage
      * @param {string} mapContext.value - Value to be processed during the map stage
      * @since 2015.2
      */

     const map = (mapContext) => {
         try {
             //log.debug('mapContext', mapContext)
             let infoCustomer = JSON.parse(mapContext.value)
             //log.debug('infoCustomer', infoCustomer)

             let week = []
             let monday = (infoCustomer.values.custentity_ptg_lunes_ == 'T') ? week.push(1) : null
             let tuesday = (infoCustomer.values.custentity_ptg_martes_ == 'T') ? week.push(2) : null
             let wednesday = (infoCustomer.values.custentity_ptg_miercoles_ == 'T') ? week.push(3) : null
             let thursday = (infoCustomer.values.custentity_ptg_jueves_ == 'T') ? week.push(4) : null
             let friday = (infoCustomer.values.custentity_ptg_viernes_ == 'T') ? week.push(5) : null
             let saturday = (infoCustomer.values.custentity_ptg_sabado_ == 'T') ? week.push(6) : null
             let sunday = (infoCustomer.values.custentity_ptg_domingo_ == 'T') ? week.push(7) : null
             //week.push(monday, tuesday, wednesday, thursday, friday, saturday, sunday);
             //log.debug('week of program services', week)
             //log.debug('week of program services', week.length)

             //Validamos si es de tipo programado o aviso
             let contactType = infoCustomer.values.custentityptg_tipodecontacto_.value;
             let clientType = (!!infoCustomer.values.custentity_ptg_tipodecliente_) ? infoCustomer.values.custentity_ptg_tipodecliente_.value : -1;
             let clienAlliance = (!!infoCustomer.values.custentity_ptg_alianza_comercial_cliente) ? infoCustomer.values.custentity_ptg_alianza_comercial_cliente.value : -1;
             //log.debug('contactType', contactType)
             if (Number(contactType) == 4 && clienAlliance == 1) {
                 //Programado
                 log.debug('data programado', infoCustomer)
                 let getWeek = new Date().getDay();
                 if (getWeek == 0) {
                     getWeek = 7;
                 }
                 //log.debug('getWeek', getWeek);
                 let customer = infoCustomer.values.internalid.value;
                 let typeService = infoCustomer.values.custentity_ptg_tipodeservicio_;
                 let existOP = validExistOp(Number(customer), Number(contactType));
                 if (week.includes(getWeek) && !existOP && !!typeService) {
                     //log.debug('infoCustomer', infoCustomer)
                     log.debug('es la semana que toca');
                     //Si tiene articulo pues se procede a crear la oportunidad
                     //log.debug('item', infoCustomer.values.custentity_ptg_articulo_frecuente)
                     if (!!infoCustomer.values.custentity_ptg_articulo_frecuente) {
                         log.debug('entro al create', 'entro')                            
                         let opportunity = record.create({
                             type: record.Type.OPPORTUNITY,
                             isDynamic: true
                         });

                         opportunity.setValue('customform', 124);
                         opportunity.setValue('entity', infoCustomer.values.internalid.value);
                         opportunity.setValue('entitystatus', 11);
                         opportunity.setValue('probability', 75);
                         // opportunity.setValue('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                         // opportunity.setValue('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
                         (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                         let day = [getWeek];
                         //log.debug('day of week', day);
                         opportunity.setValue('custbody_ptg_dia_semana', day);
                         opportunity.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                         let today = new Date();
                         //today = today.setDate(today.getDate()+1);
                         today.setDate(today.getDate() + 1);
                         //log.debug('today add', today)
                         opportunity.setValue('expectedclosedate', today)
                         opportunity.insertLine({ sublistId: 'item', line: 0 });
                         opportunity.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values.custentity_ptg_articulo_frecuente.value) });                            
                         opportunity.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });                                                        
                         opportunity.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: 200 });
                         opportunity.commitLine({ sublistId: 'item' });
                         let id = opportunity.save();
                         log.debug('se creo programado', id)

                     }
                 }

             }
             else if (Number(contactType) == 2 && clienAlliance == 2 || clientType == 5) {
                 //Aviso                    
                 log.debug('data aviso', infoCustomer)
                 let getWeek = new Date().getDay();
                 if (getWeek == 0) {
                     getWeek = 7;
                 }
                 let nextWeek = (getWeek == 7) ? 1 : getWeek + 1;
                 let customer = infoCustomer.values.internalid.value;
                 let existOP = validExistOp(Number(customer), Number(contactType));
                 let typeService = infoCustomer.values.custentity_ptg_tipodeservicio_;
                 if (week.includes(getWeek) || week.includes(nextWeek) && !existOP && !!typeService) {
                     //log.debug('infoCustomer', infoCustomer)
                     log.debug('es la semana que toca avisar');
                     switch (typeService.value) {
                         case '1':
                             let itemCilindro = getGenericItem(typeService.value, infoCustomer.values.subsidiary.value)
                             log.debug('itemCilindro', itemCilindro)
                             if (!!itemCilindro) {
                                 let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                                 log.debug('priceZoneValue Cilindro', priceZoneValue)
                                 let itemContent = search.lookupFields({
                                     type: 'item',
                                     id: Number(itemCilindro),
                                     columns: ['custitem_ptg_capacidadcilindro_']
                                 });
                                 log.debug('itemcontent', itemContent)
                                 let opportunityCilindro = record.create({
                                     type: record.Type.OPPORTUNITY,
                                     isDynamic: true
                                 });

                                 opportunityCilindro.setValue('customform', 124);
                                 opportunityCilindro.setValue('entity', infoCustomer.values.internalid.value);
                                 opportunityCilindro.setValue('entitystatus', 11);
                                 opportunityCilindro.setValue('probability', 75);
                                 // opportunityCilindro.setValue('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                                 // opportunityCilindro.setValue('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
                                 (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                                 let day = [getWeek];
                                 //log.debug('day of week', day);
                                 opportunityCilindro.setValue('custbody_ptg_dia_semana', day);
                                 opportunityCilindro.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                                 let today = new Date();
                                 //today = today.setDate(today.getDate()+1);
                                 today.setDate(today.getDate() + 1);
                                 //og.debug('today add', today)
                                 opportunityCilindro.setValue('expectedclosedate', today)

                                 if (!!itemContent['custitem_ptg_capacidadcilindro_'] && !!priceZoneValue) {
                                     opportunityCilindro.insertLine({ sublistId: 'item', line: 0 });
                                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(itemCilindro) });
                                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });
                                     let finalAmount = (Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) * Number(itemContent['custitem_ptg_capacidadcilindro_'])) * Number(priceZoneValue);
                                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });
                                     opportunityCilindro.commitLine({ sublistId: 'item' });
                                     let id = opportunityCilindro.save();
                                     log.debug('se creo aviso cilindro', id)
                                 }


                             }

                             break;

                         case '2':
                             let itemEstacionaria = getGenericItem(typeService.value, infoCustomer.values.subsidiary.value)
                             log.debug('itemEstacionaria', itemEstacionaria)
                             if (!!itemEstacionaria) {
                                 let priceZoneValue = getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value);
                                 let opportunityEstacionaria = record.create({
                                     type: record.Type.OPPORTUNITY,
                                     isDynamic: true
                                 });

                                 opportunityEstacionaria.setValue('customform', 124);
                                 opportunityEstacionaria.setValue('entity', infoCustomer.values.internalid.value);
                                 opportunityEstacionaria.setValue('entitystatus', 11);
                                 opportunityEstacionaria.setValue('probability', 75);
                                 // opportunityEstacionaria.setValue('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                                 // opportunityEstacionaria.setValue('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
                                 (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                                 let day = [getWeek];
                                 //log.debug('day of week', day);
                                 opportunityEstacionaria.setValue('custbody_ptg_dia_semana', day);
                                 opportunityEstacionaria.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                                 let today = new Date();
                                 //today = today.setDate(today.getDate()+1);
                                 today.setDate(today.getDate() + 1);
                                 //log.debug('today add', today)
                                 opportunityEstacionaria.setValue('expectedclosedate', today)

                                 if (!!priceZoneValue) {
                                     opportunityEstacionaria.insertLine({ sublistId: 'item', line: 0 });
                                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(itemEstacionaria) });
                                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });
                                     let finalAmount = Number(priceZoneValue) * Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil);
                                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value : finalAmount});
                                     opportunityEstacionaria.commitLine({ sublistId: 'item' });
                                     let id = opportunityEstacionaria.save();
                                     log.debug('se creo aviso estacionaria', id)
                                 }


                             }
                             break;

                         //Este queda pendiente del global de momento se dejara 4 , hasta que se cambie el nombre
                         case '4':
                             break;

                     }

                 }
                 // else {
                 //     log.debug('no es la semana que te toca avisar');
                 // }

             }

         } catch (error) {
             log.debug('err map', error)
         }

     }


     const validExistOp = (customer, type) => {

         let isExiste = false;
         let filters;
         if (type == 4) {
             filters = [
                 ["entity", "anyof", customer],
                 "AND",
                 ["date", "within", "today"],
                 "OR",
                 ["date", "within", "tomorrow"],
                 "AND",
                 ["entitystatus", "anyof", "10", "11", "13", "18", "14"]
             ]
         } else {
             filters = [
                 ["entity", "anyof", customer],
                 "AND",
                 ["date", "within", "today"],
                 "AND",
                 ["entitystatus", "anyof", "10", "11", "13", "18", "14"]
             ]
         }

         let opportunitySearchObj = search.create({
             type: "opportunity",
             filters: filters,
             columns:
                 [
                     search.createColumn({ name: "internalid", label: "Internal ID" }),
                     search.createColumn({ name: "transactionnumber", label: "Transaction Number" })
                 ]
         });
         let searchResultCount = opportunitySearchObj.runPaged().count;

         if (searchResultCount > 0) {
             isExiste = true;

         }
         return isExiste
     }

     const getGenericItem = (type, subsidiary) => {
         let filters;
         switch (type) {
             case '1':
                 filters = [["type", "anyof", "InvtPart"],
                     "AND",
                 ["custitem_ptg_tipodearticulo_", "anyof", "1"],
                     "AND",
                 ["subsidiary", "anyof", subsidiary]]
                 break;
             case '2':
                 filters = [["type", "anyof", "InvtPart"],
                     "AND",
                 ["custitem_ptg_tipodearticulo_", "anyof", "2"],
                     "AND",
                 ["subsidiary", "anyof", subsidiary]]
                 break;

             //Este queda pendiente del global de momento se dejara 4 , hasta que se cambie el nombre
             case '4':
                 filters = [["type", "anyof", "InvtPart"],
                     "AND",
                 ["custitem_ptg_tipodearticulo_", "anyof", "1"],
                     "OR",
                 ["custitem_ptg_tipodearticulo_", "anyof", "2"],
                     "AND",
                 ["subsidiary", "anyof", subsidiary]]
                 break;

         }

         //log.debug('filtros articulos genericos', filters)

         let inventoryitemSearchObj = search.create({
             type: "inventoryitem",
             filters: filters,
             columns:
                 [
                     search.createColumn({
                         name: "itemid",
                         sort: search.Sort.ASC,
                         label: "Name"
                     }),
                     search.createColumn({ name: "displayname", label: "Display Name" }),
                     search.createColumn({ name: "salesdescription", label: "Description" }),
                     search.createColumn({ name: "type", label: "Type" }),
                     search.createColumn({ name: "baseprice", label: "Base Price" }),
                     search.createColumn({ name: "custitem_4601_defaultwitaxcode", label: "Default WT Code" })
                 ]
         });
         let searchResultCount = inventoryitemSearchObj.runPaged().count;
         if (searchResultCount > 0) {
             let item;
             inventoryitemSearchObj.run().each(function (result) {
                 // .run().each has a limit of 4,000 results
                 item = result.id
                 log.debug('result', result)

                 return false;
             });

             return item;
         }

     }

     const getPriceZone = (zone) => {
         log.debug('zone', zone);
         let customrecord_ptg_coloniasrutas_SearchObj = search.create({
             type: "customrecord_ptg_coloniasrutas_",
             filters:
                 [
                     ["internalid", "anyof", zone]
                 ],
             columns:
                 [
                     search.createColumn({
                         name: "custrecord_ptg_precio_",
                         join: "CUSTRECORDPTG_ZONA_DE_PRECIO",
                         label: "PTG - PRECIO"
                     }),
                     search.createColumn({
                         name: "custrecord_ptg_territorio_",
                         join: "CUSTRECORDPTG_ZONA_DE_PRECIO",
                         label: "PTG - Territorio"
                     }),
                     search.createColumn({ name: "custrecordptg_zona_de_precio", label: "PTG - Zona de Precio" })
                 ]
         });
         let searchResultCount = customrecord_ptg_coloniasrutas_SearchObj.runPaged().count;
         if (searchResultCount > 0) {
             let price;
             customrecord_ptg_coloniasrutas_SearchObj.run().each(function (result) {
                 // .run().each has a limit of 4,000 results
                 log.debug('result zone', result)
                 price = result.getValue({
                     name: "custrecord_ptg_precio_",
                     join: "CUSTRECORDPTG_ZONA_DE_PRECIO",
                     label: "PTG - PRECIO"
                 })
                 return true;
             });

             return price;
         }


     }

     /**
      * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
      * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
      * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
      *     provided automatically based on the results of the map stage.
      * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
      *     reduce function on the current group
      * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
      * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
      *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
      * @param {string} reduceContext.key - Key to be processed during the reduce stage
      * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
      *     for processing
      * @since 2015.2
      */
     const reduce = (reduceContext) => {

     }


     /**
      * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
      * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
      * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
      * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
      *     script
      * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
      * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
      *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
      * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
      * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
      * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
      *     script
      * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
      * @param {Object} summaryContext.inputSummary - Statistics about the input stage
      * @param {Object} summaryContext.mapSummary - Statistics about the map stage
      * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
      * @since 2015.2
      */
     const summarize = (summaryContext) => {
         const thereAreAnyError = (summaryContext) => {
             const inputSummary = summaryContext.inputSummary;
             const mapSummary = summaryContext.mapSummary;
             const reduceSummary = summaryContext.reduceSummary;
             //si no hay errores entonces se sale del la función y se retorna false incando que no hubo errores            if (!inputSummary.error) return false;
             //se hay errores entonces se imprimen los errores en el log para poder visualizarlos            if (inputSummary.error) log.debug("ERROR_INPPUT_STAGE", `Erro: ${inputSummary.error}`);
             handleErrorInStage('map', mapSummary);
             handleErrorInStage('reduce', reduceSummary);

             function handleErrorInStage(currentStage, summary) {
                 summary.errors.iterator().each((key, value) => {
                     log.debug(`ERROR_${currentStage}`, `Error( ${currentStage} ) with key: ${key}.Detail: ${JSON.parse(value).message}`);
                     return true;
                 });
             }
             return true;
         };

         thereAreAnyError(summaryContext)
         log.audit('Summary', [
             { title: 'Usage units consumed', details: summaryContext.usage },
             { title: 'Concurrency', details: summaryContext.concurrency },
             { title: 'Number of yields', details: summaryContext.yields }
         ]);

     }

     return { getInputData, map, reduce, summarize }

 });
