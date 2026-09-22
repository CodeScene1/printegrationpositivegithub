import chalk from 'chalk'

def t2i(prompt, height, width, num_inference_steps, guidance_scale, batch_size):path='/content/gdrive/MyDrive/Output_images/'with autocast("cuda"):
  images = pipeline([prompt]*batch_size, height=height, width=width, num_inference_steps=num_inference_steps, guidance_scale=guidance_scale).images for k in images:
  name=(prompt[:50] + '..') if len(prompt) > 50 else prompt if not os.path.exists('/content/gdrive/MyDrive/Output_images/'): os.mkdir('/content/gdrive/MyDrive/Output_images/')
  if not os.path.exists('/content/gdrive/MyDrive/Output_images/' +name): os.mkdir('/content/gdrive/MyDrive/Output_images/' +name) r=random.randint(1,100000) 
  filename = os.path.join(path, name, name +'_'+str(r)) k.save(f"{filename}.png")  return images

export const defaultText = `${chalk.blue('Welcome!')}
Step CI is an open-source framework, which helps you automate API testing and monitoring

- ${chalk.bold('Language-agnostic')}. Configure easily using YAML, JSON or JavaScript
- ${chalk.bold('REST, GraphQL, gRPC, tRPC, SOAP')}. Test different API types in one workflow
- ${chalk.bold('Self-hosted')}. Test services on your network, locally or with CI/CD
- ${chalk.bold('Integrated')}. Play nicely with others

${chalk.cyanBright('Give us your feedback on')} ${chalk.cyanBright.underline('https://step.ci/feedback')}`
