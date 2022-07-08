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
                         }),
                         search.createColumn({ name: "datecreated", label: "Date Created" }),
                         search.createColumn({ name: "custentity_ptg_cada_", label: "PTG - Cada:" })
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

             //Validamos que sea el tipo programado o aviso
             if (Number(contactType) == 4 && (clienAlliance == 1 || clienAlliance == 2 || clientType == 5)) {
                 log.debug('progromado', infoCustomer)
                 //Validar si es día, semana o mensual
                 let typeService = infoCustomer.values.custentity_ptg_tipodeservicio_;
                 let typeFrequency = infoCustomer.values.custentity_ptg_periododecontacto_.value;
                 let customer = infoCustomer.values.internalid.value;
                 let existOP = validExistOp(Number(customer), Number(contactType));
                 switch (infoCustomer.values.custentity_ptg_periododecontacto_.value) {
                     //Dias
                     case "1":
                         log.debug('dia', infoCustomer.values.custentity_ptg_periododecontacto_.value)
                         //Validar si le toca hoy el servicio 
                         let makeServiceDay = validService(typeFrequency, week, infoCustomer.values.datecreated);
                         log.debug('makeServiceDay', makeServiceDay)
                         //Validamos el tipo de servicio si es cilindro , estacionario o mixto
                         log.debug('existOP', existOP)
                         if (!!typeService && Number(typeService.value) == 1 && makeServiceDay && !existOP) {
                             log.debug('typeService', 'clindro')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 2 && makeServiceDay && !existOP) {
                             log.debug('typeService', 'estacionario')
                             makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 4 && makeServiceDay && !existOP) {
                             log.debug('typeService', 'ambos')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         }

                         break;
                     //Semanal
                     case "2":
                         log.debug('semana', infoCustomer.values.custentity_ptg_periododecontacto_.value)
                         //Validar si le toca hoy el servicio                             
                         let makeServiceWeek = validService(typeFrequency, week, infoCustomer.values.datecreated, Number(infoCustomer.values.custentity_ptg_cada_));
                         log.debug('makeservice', makeServiceWeek)
                         //Validamos el tipo de servicio si es cilindro , estacionario o mixto y que no tenga creada una oportunidad                                                                                    
                         log.debug('existOP', existOP)
                         if (!!typeService && Number(typeService.value) == 1 && makeServiceWeek && !existOP) {
                             log.debug('typeService', 'clindro')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 2 && makeServiceWeek && !existOP) {
                             log.debug('typeService', 'estacionario')
                             makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 4 && makeServiceWeek && !existOP) {
                             log.debug('typeService', 'ambos')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         }

                         break;
                     //Mensual
                     case "3":
                         log.debug('mensual', infoCustomer.values.custentity_ptg_periododecontacto_.value)
                         //Validar si le toca hoy el servicio 
                         let makeServiceMounth = validService(typeFrequency, week, infoCustomer.values.datecreated);
                         log.debug('makeServiceMounth', makeServiceMounth)
                         //Validamos el tipo de servicio si es cilindro , estacionario o mixto                         
                         if (!!typeService && Number(typeService.value) == 1 && makeServiceMounth && !existOP) {
                             log.debug('typeService', 'clindro')
                             makeOPCilindro(typeService, infoCustomer);
                         } else if (!!typeService && Number(typeService.value) == 2 && makeServiceMounth && !existOP) {
                             log.debug('typeService', 'estacionario')
                             makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 4 && makeServiceMounth && !existOP) {
                             log.debug('typeService', 'ambos')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         }

                         break;
                 }

             } else if (Number(contactType) == 2) {
                 log.debug('aviso', infoCustomer)
                 let typeService = infoCustomer.values.custentity_ptg_tipodeservicio_;
                 let typeFrequency = infoCustomer.values.custentity_ptg_periododecontacto_.value;
                 let customer = infoCustomer.values.internalid.value;
                 let existOP = validExistOp(Number(customer), Number(contactType));
                 switch (infoCustomer.values.custentity_ptg_periododecontacto_.value) {
                     //Dias
                     case "1":
                         log.debug('dia', infoCustomer.values.custentity_ptg_periododecontacto_.value)
                         //Validar si le toca hoy el servicio 
                         let makeServiceDay = validService(typeFrequency, week, infoCustomer.values.datecreated);
                         log.debug('makeServiceDay', makeServiceDay)
                         //Validamos el tipo de servicio si es cilindro , estacionario o mixto
                         if (!!typeService && Number(typeService.value) == 1 && makeServiceDay && !existOP) {
                             log.debug('typeService', 'clindro')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 2 && makeServiceDay && !existOP) {
                             log.debug('typeService', 'estacionario')
                             makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 4 && makeServiceDay && !existOP) {
                             log.debug('typeService', 'ambos')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         }

                         break;
                     //Semanal
                     case "2":
                         log.debug('semana', infoCustomer.values.custentity_ptg_periododecontacto_.value)
                         //Validar si le toca hoy el servicio                             
                         let makeServiceWeek = validServiceAviso(typeFrequency, week, infoCustomer.values.datecreated, Number(infoCustomer.values.custentity_ptg_cada_));
                         log.debug('makeservice', makeServiceWeek)
                         //Validamos el tipo de servicio si es cilindro , estacionario o mixto y que no tenga creada una oportunidad                                                                                    
                         log.debug('existOP', existOP)
                         if (!!typeService && Number(typeService.value) == 1 && makeServiceWeek && !existOP) {
                             log.debug('typeService', 'clindro')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 2 && makeServiceWeek && !existOP) {
                             log.debug('typeService', 'estacionario')
                             makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 4 && makeServiceWeek && !existOP) {
                             log.debug('typeService', 'ambos')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         }

                         break;
                     //Mensual
                     case "3":
                         log.debug('mensual', infoCustomer.values.custentity_ptg_periododecontacto_.value)
                         //Validar si le toca hoy el servicio 
                         let makeServiceMounth = validService(typeFrequency, week, infoCustomer.values.datecreated);
                         log.debug('makeServiceMounth', makeServiceMounth)
                         //Validamos el tipo de servicio si es cilindro , estacionario o mixto                         
                         if (!!typeService && Number(typeService.value) == 1 && makeServiceMounth && !existOP) {
                             log.debug('typeService', 'clindro')
                             makeOPCilindro(typeService, infoCustomer);
                         } else if (!!typeService && Number(typeService.value) == 2 && makeServiceMounth && !existOP) {
                             log.debug('typeService', 'estacionario')
                             makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                         } else if (!!typeService && Number(typeService.value) == 4 && makeServiceMounth && !existOP) {
                             log.debug('typeService', 'ambos')
                             makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                         }

                         break;
                 }
             }
             
         } catch (error) {
             log.debug('err map', error)
         }

     }

     //Validar el tipo de servicio y frecuencia
     const validService = (type, weeks, date, frequency) => {
         let day = new Date();

         switch (type) {
             //Dias
             case "1":
                 let days = (day.getDay == 0) ? 7 : day.getDay();
                 if (weeks.includes(days)) {
                     return true;
                 }
                 break;
             //Semanal
             case "2":
                 let week = (day.getDay == 0) ? 7 : day.getDay();
                 let initWeek = getWeek(date);
                 let today = getWeek(day);
                 log.debug('initWeek', initWeek);
                 log.debug('today', today);
                 let resultMultiple = isMultiple(initWeek, today, frequency);
                 log.debug('resultMultiple', resultMultiple)
                 if (weeks.includes(week) && resultMultiple) {
                     return true;
                 }
                 break;
             //Mensual
             case "3":
                 let initialFormattedDateString = date;
                 //log.debug('initialFormattedDateString', initialFormattedDateString)
                 let parsedDateStringAsRawDateObject = format.parse({
                     value: initialFormattedDateString,
                     type: format.Type.DATE,
                     timezone: format.Timezone.AMERICA_MEXICO_CITY
                 });

                 let initDate = parsedDateStringAsRawDateObject.getDate();
                 let todayMounth = day.getDate();
                 log.debug('initDate mounth', initDate)
                 log.debug('todayMounth mounth', todayMounth)
                 if (initDate == todayMounth) {
                     return true;
                 }
                 break;
         }

     }

     //Este tiene la especial de ser para aviso semanal, ya que si la semana no se encuentra hoy pero mañana si, se tiene que crear
     const validServiceAviso = (type, weeks, date, frequency) => {
         let day = new Date();

         switch (type) {
             //Dias
             case "1":
                 let days = (day.getDay == 0) ? 7 : day.getDay();
                 if (weeks.includes(days)) {
                     return true;
                 }
                 break;
             //Semanal
             case "2":
                 let week = (day.getDay == 0) ? 7 : day.getDay();
                 let nextWeek = (week == 7) ? 1 : week + 1;
                 let initWeek = getWeek(date);
                 let today = getWeek(day);
                 log.debug('initWeek', initWeek);
                 log.debug('today', today);
                 let resultMultiple = isMultiple(initWeek, today, frequency);
                 log.debug('resultMultiple', resultMultiple)
                 if ((weeks.includes(week) || weeks.includes(nextWeek) )&& resultMultiple) {
                     return true;
                 }
                 break;
             //Mensual
             case "3":
                 let initialFormattedDateString = date;
                 //log.debug('initialFormattedDateString', initialFormattedDateString)
                 let parsedDateStringAsRawDateObject = format.parse({
                     value: initialFormattedDateString,
                     type: format.Type.DATE,
                     timezone: format.Timezone.AMERICA_MEXICO_CITY
                 });

                 let initDate = parsedDateStringAsRawDateObject.getDate();
                 let todayMounth = day.getDate();
                 log.debug('initDate mounth', initDate)
                 log.debug('todayMounth mounth', todayMounth)
                 if (initDate == todayMounth) {
                     return true;
                 }
                 break;
         }

     }

     const getWeek = (date) => {
         let initialFormattedDateString = date;
         log.debug('initialFormattedDateString', initialFormattedDateString)
         let parsedDateStringAsRawDateObject = format.parse({
             value: initialFormattedDateString,
             type: format.Type.DATE,
             timezone: format.Timezone.AMERICA_MEXICO_CITY
         });
         //log.debug('date created transform', parsedDateStringAsRawDateObject)
         // log.debug('date created get year', parsedDateStringAsRawDateObject.getFullYear())
         let oneJan = new Date(parsedDateStringAsRawDateObject.getFullYear(), 0, 1);
         let numberOfDays = Math.floor((parsedDateStringAsRawDateObject - oneJan) / (24 * 60 * 60 * 1000));
         let resultWeek = Math.ceil((parsedDateStringAsRawDateObject.getDay() + 1 + numberOfDays) / 7);
         log.debug('result week of year', resultWeek)
         return resultWeek;
     }

     const isMultiple = (initWeek, today, frequency) => {
         let result = false;
         if ((today - initWeek) % frequency == 0) {
             result = true;
         }
         return result;
     }

     //Funciones para crear clindros
     const makeOPCilindro = (typeService, infoCustomer, contactType) => {
         log.debug('makeOPCilindro', 'entro')
         let getWeek = new Date().getDay();
         if (getWeek == 0) {
             getWeek = 7;
         }

         if (contactType == 4 && (Number(typeService) == 1 || Number(typeService) == 4)) {
             //Validamos que tenga configurado un articulo
             if (!!infoCustomer.values.custentity_ptg_articulo_frecuente) {
                 //Obtenemos el precio por zona
                 let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                 log.debug('priceZoneValue Cilindro', priceZoneValue)
                 //Obtenemos la cantidad que tiene el articulo de cilindro
                 let itemContent = search.lookupFields({
                     type: 'item',
                     id: Number(infoCustomer.values.custentity_ptg_articulo_frecuente.value),
                     columns: ['custitem_ptg_capacidadcilindro_']
                 });
                 log.debug('itemcontent', itemContent)
                 //Creamos la oportunidad
                 let opportunityCilindro = record.create({
                     type: record.Type.OPPORTUNITY,
                     isDynamic: true
                 });

                 opportunityCilindro.setValue('customform', 124);
                 opportunityCilindro.setValue('entity', infoCustomer.values.internalid.value);
                 opportunityCilindro.setValue('entitystatus', 10);
                 opportunityCilindro.setValue('probability', 75);
                 opportunityCilindro.setValue('custbody_ptg_type_service', 4);
                 opportunityCilindro.setText('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                 opportunityCilindro.setText('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
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

                 //Validamos que tenga una capacidad del cilindro y que tenga configurado un precio de zona
                 if (!!itemContent['custitem_ptg_capacidadcilindro_'] && Number(priceZoneValue) > 0) {
                     opportunityCilindro.insertLine({ sublistId: 'item', line: 0 });
                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values.custentity_ptg_articulo_frecuente.value) });
                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });
                     let finalRate = Number(itemContent['custitem_ptg_capacidadcilindro_']) * Number(priceZoneValue);
                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: finalRate });
                     // let finalAmount = (Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) * Number(itemContent['custitem_ptg_capacidadcilindro_'])) * Number(priceZoneValue);
                     // opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });
                     opportunityCilindro.commitLine({ sublistId: 'item' });
                     let id = opportunityCilindro.save();
                     log.debug('se creo cilindro programada', id)
                 }
             }


         } else if (contactType == 2 && (Number(typeService) == 1 || Number(typeService) == 4)) {
             //Buscamos un articulo generico para poner de momento
             let itemCilindro = getGenericItem(typeService, infoCustomer.values.subsidiary.value)
             log.debug('itemCilindro', itemCilindro)
             //Verificamos que busco un articulo
             if (!!itemCilindro) {
                 //Obtenemos el precio por ubicacion
                 let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                 log.debug('priceZoneValue Cilindro', priceZoneValue)
                 //Obtenemos la capacidad del cilindro
                 let itemContent = search.lookupFields({
                     type: 'item',
                     id: Number(itemCilindro),
                     columns: ['custitem_ptg_capacidadcilindro_']
                 });
                 log.debug('itemcontent', itemContent)
                 //Creamos la op
                 let opportunityCilindro = record.create({
                     type: record.Type.OPPORTUNITY,
                     isDynamic: true
                 });

                 opportunityCilindro.setValue('customform', 124);
                 opportunityCilindro.setValue('entity', infoCustomer.values.internalid.value);
                 opportunityCilindro.setValue('entitystatus', 10);
                 opportunityCilindro.setValue('probability', 75);
                 opportunityCilindro.setValue('custbody_ptg_type_service', 4);
                 opportunityCilindro.setText('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                 opportunityCilindro.setText('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
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

                 //Validamos que tenga configurado la capacidad y el precio de zona
                 if (!!itemContent['custitem_ptg_capacidadcilindro_'] && Number(priceZoneValue) > 0) {
                     opportunityCilindro.insertLine({ sublistId: 'item', line: 0 });
                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(itemCilindro) });
                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });
                     let finalRate = Number(itemContent['custitem_ptg_capacidadcilindro_']) * Number(priceZoneValue);
                     opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: finalRate });
                     // let finalAmount = (Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) * Number(itemContent['custitem_ptg_capacidadcilindro_'])) * Number(priceZoneValue);
                     // opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });
                     opportunityCilindro.commitLine({ sublistId: 'item' });
                     let id = opportunityCilindro.save();
                     log.debug('se creo cilindro aviso', id)
                 }


             }

         }
     }

     //Funcion para crear estacionario
     const makeOPEstacionario = (typeService, infoCustomer, contactType) => {
         log.debug('makeOPEstacionario', 'entro')
         let getWeek = new Date().getDay();
         if (getWeek == 0) {
             getWeek = 7;
         }

         if (contactType == 4 && (Number(typeService) == 2 || Number(typeService) == 4)) {
             //Mismo proceso que cilindro pero ahora con estacionario y la diferencia de momento es que no tiene configurado una capacidad por el tipo de articulo
             if (!!infoCustomer.values.custentity_ptg_articulo_frecuente) {
                 let priceZoneValue = getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value);
                 let opportunityEstacionaria = record.create({
                     type: record.Type.OPPORTUNITY,
                     isDynamic: true
                 });

                 opportunityEstacionaria.setValue('customform', 124);
                 opportunityEstacionaria.setValue('entity', infoCustomer.values.internalid.value);
                 opportunityEstacionaria.setValue('entitystatus', 10);
                 opportunityEstacionaria.setValue('probability', 75);
                 opportunityEstacionaria.setValue('custbody_ptg_type_service', 1);
                 opportunityEstacionaria.setText('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                 opportunityEstacionaria.setText('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
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

                 if (Number(priceZoneValue) > 0) {
                     opportunityEstacionaria.insertLine({ sublistId: 'item', line: 0 });
                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values.custentity_ptg_articulo_frecuente.value) });
                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });
                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: Number(priceZoneValue) });
                     // let finalAmount = Number(priceZoneValue) * Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil);
                     // opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });                                        
                     opportunityEstacionaria.commitLine({ sublistId: 'item' });
                     let id = opportunityEstacionaria.save();
                     log.debug('se creo programado estacionaria', id)
                 }
             }

         }
         else if (contactType == 2 && (Number(typeService) == 2 || Number(typeService) == 4)) {
             let itemEstacionaria = getGenericItem(typeService, infoCustomer.values.subsidiary.value)
             log.debug('itemEstacionaria', itemEstacionaria)
             if (!!itemEstacionaria) {
                 let priceZoneValue = getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value);
                 let opportunityEstacionaria = record.create({
                     type: record.Type.OPPORTUNITY,
                     isDynamic: true
                 });

                 opportunityEstacionaria.setValue('customform', 124);
                 opportunityEstacionaria.setValue('entity', infoCustomer.values.internalid.value);
                 opportunityEstacionaria.setValue('entitystatus', 10);
                 opportunityEstacionaria.setValue('probability', 75);
                 opportunityEstacionaria.setValue('custbody_ptg_type_service', 1);
                 opportunityEstacionaria.setText('custbody_ptg_entre_las', infoCustomer.values.custentity_ptg_entrelas_);
                 opportunityEstacionaria.setText('custbody_ptg_y_las', infoCustomer.values.custentity_ptg_ylas_);
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

                 if (Number(priceZoneValue) > 0) {
                     opportunityEstacionaria.insertLine({ sublistId: 'item', line: 0 });
                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(itemEstacionaria) });
                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) });
                     opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: Number(priceZoneValue) });
                     // let finalAmount = Number(priceZoneValue) * Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil);
                     // opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });
                     opportunityEstacionaria.commitLine({ sublistId: 'item' });
                     let id = opportunityEstacionaria.save();
                     log.debug('se creo aviso estacionaria', id)
                 }

             }
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
                 ["entitystatus", "anyof", "19", "11", "10"]
             ]
         } else if (type == 2) {
             filters = [
                 ["entity", "anyof", customer],
                 "AND",
                 ["date", "within", "today"],
                 "AND",
                 ["entitystatus", "anyof", "19", "11", "10"]
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
             log.debug('searchresult exist', searchResultCount);
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
