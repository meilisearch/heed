use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, FnArg, Ident, ItemFn, Pat, PatType, Type};

#[proc_macro_attribute]
pub fn mut_read_txn(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the function
    let mut input_fn = parse_macro_input!(item as ItemFn);

    // Get the parameter name from the attribute
    let param_name = parse_macro_input!(attr as Ident);

    // Flag to check if we found and modified the parameter
    let mut found_and_modified = false;

    // Iterate through the function arguments
    for arg in &mut input_fn.sig.inputs {
        if let FnArg::Typed(PatType { pat, ty, .. }) = arg {
            // Check if this is the parameter we want to modify
            if let Pat::Ident(pat_ident) = pat.as_ref() {
                if pat_ident.ident == param_name {
                    if let Type::Reference(type_reference) = ty.as_mut() {
                        if let Type::Path(type_path) = type_reference.elem.as_mut() {
                            if let Some(segment) = type_path.path.segments.last() {
                                if segment.ident == "RoTxn" {
                                    // Check if it's non-mutable
                                    if type_reference.mutability.is_none() {
                                        // Add the `mut` keyword
                                        type_reference.mutability = Some(parse_quote!(mut));
                                        found_and_modified = true;
                                    } else {
                                        // If it's already mutable, return an error
                                        return syn::Error::new_spanned(
                                            type_reference,
                                            "The specified parameter is already mutable",
                                        )
                                        .to_compile_error()
                                        .into();
                                    }
                                } else {
                                    // If it's not RoTxn, return an error
                                    return syn::Error::new_spanned(
                                        type_path,
                                        "The specified parameter is not of type RoTxn",
                                    )
                                    .to_compile_error()
                                    .into();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // If we didn't find and modify the parameter, return an error
    if !found_and_modified {
        return syn::Error::new_spanned(
            input_fn.sig,
            format!("Could not find non-mutable parameter '{}' of type RoTxn", param_name),
        )
        .to_compile_error()
        .into();
    }

    // Generate the modified function
    let output = quote! {
        #input_fn
    };

    output.into()
}
